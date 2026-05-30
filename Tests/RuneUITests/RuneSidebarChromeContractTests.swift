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
                    onSaveAllTerminalTranscripts: {}
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
                onSaveAllTranscripts: {}
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

    func testTerminalWorkspaceComposesIndependentPanelsInStableOrder() throws {
        let workspaceSource = try String(contentsOfFile: resourceTerminalInspectorViewPath, encoding: .utf8)
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let shellSource = try String(contentsOfFile: terminalShellPanelViewPath, encoding: .utf8)
        let portForwardSource = try String(contentsOfFile: terminalPortForwardPanelViewPath, encoding: .utf8)
        let tabBarSource = try String(contentsOfFile: terminalSessionTabBarPath, encoding: .utf8)
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
        XCTAssertTrue(portForwardSource.contains("@Binding var isExpanded"))
        XCTAssertTrue(portForwardSource.contains("compactStatus"))
        XCTAssertTrue(portForwardSource.contains("expandedControls"))
        XCTAssertTrue(rootViewSource.contains("enum TerminalInspectorTab"))
        XCTAssertTrue(rootViewSource.contains("case commands"))
        XCTAssertTrue(rootViewSource.contains("case logs"))
        XCTAssertTrue(rootViewSource.contains("case yaml"))
        XCTAssertTrue(rootViewSource.contains("selection: $terminalInspectorTab"))
        XCTAssertTrue(rootViewSource.contains("PodLogsInspectorPane("))
        XCTAssertTrue(rootViewSource.contains("manifestInspectorPane(activeTab: .yaml)"))
        XCTAssertTrue(rootViewSource.contains("viewModel.focusTerminalPodInspector"))
        XCTAssertTrue(tabBarSource.contains("ScrollView(.horizontal"))
        XCTAssertTrue(tabBarSource.contains("accessibilityLabel(\"New Shell\")"))
        XCTAssertTrue(sessionControlSource.contains("struct TerminalSessionControlRow"))
        XCTAssertTrue(transcriptSource.contains("struct TerminalTranscriptSurface"))
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
        XCTAssertTrue(terminalLogsBlock.contains("let podOptions = terminalLogPodOptions(namespace: pod.namespace)"))
        XCTAssertTrue(terminalLogsBlock.contains("podOptions: podOptions"))
        XCTAssertTrue(terminalLogsBlock.contains("selectedPodID: terminalLogPodSelectionBinding(currentPod: pod, podOptions: podOptions)"))
        XCTAssertTrue(terminalLogsBlock.contains("showsContainerPicker: false"))
        XCTAssertTrue(terminalLogsBlock.contains("viewModel.focusTerminalPodInspector(pod, reloadLogs: true)"))
        XCTAssertFalse(terminalLogsBlock.contains("UnifiedResourceLogsInspectorPane"))

        XCTAssertTrue(rootViewSource.contains("private func terminalLogPodOptions(namespace: String) -> [PodSummary]"))
        XCTAssertTrue(rootViewSource.contains(".filter { $0.namespace == namespace }"))
        XCTAssertTrue(rootViewSource.contains("private func terminalLogPodSelectionBinding(currentPod: PodSummary, podOptions: [PodSummary]) -> Binding<String>"))
        XCTAssertTrue(rootViewSource.contains("terminalShellPodID = pod.id"))
        XCTAssertTrue(rootViewSource.contains("viewModel.focusTerminalPodInspector(pod, reloadLogs: true)"))

        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let terminalFocusBlock = try functionBlock(
            named: "public func focusTerminalPodInspector",
            endingBefore: "private func selectPod",
            in: viewModelSource
        )
        XCTAssertTrue(terminalFocusBlock.contains("selectedLogContainer = \"\""))
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

        let podPopup = allPopUpButtons(in: host.view).first { popup in
            let titles = Set(popup.itemArray.map(\.title))
            return pods.allSatisfy { titles.contains($0.name) }
        }

        XCTAssertNotNil(podPopup, "Expected logs toolbar pod picker to list every pod option in the namespace.")
        XCTAssertFalse(
            allPopUpButtons(in: host.view).contains { popup in
                let titles = Set(popup.itemArray.map(\.title))
                return titles.contains("All containers") || titles.contains("app")
            },
            "Terminal logs should show pod selection only, not a second container dropdown."
        )

        let logsInspectorSource = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        XCTAssertTrue(logsInspectorSource.contains("case \"Pod\":\n            return 240"))
        XCTAssertTrue(logsInspectorSource.contains(".frame(minHeight: RuneUILayoutMetrics.inspectorToolbarGroupMinHeight)"))
    }

    func testTerminalTabsUseFullTabHitAreaForSelection() throws {
        let tabBarSource = try String(contentsOfFile: terminalSessionTabBarPath, encoding: .utf8)

        XCTAssertTrue(tabBarSource.contains(".frame(width: 216, height: 28"))
        XCTAssertTrue(tabBarSource.contains(".contentShape(RoundedRectangle(cornerRadius: 6"))
        XCTAssertTrue(tabBarSource.contains(".onTapGesture"))
        XCTAssertTrue(tabBarSource.contains("select(session)"))
        XCTAssertTrue(tabBarSource.contains(".frame(width: 28, height: 28)"))
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
        XCTAssertTrue(rootSource.contains("workspaceChromeMountDelayNanoseconds: UInt64 = 120_000_000"))
        XCTAssertTrue(rootSource.contains(".allowsHitTesting(false)"))
        XCTAssertTrue(rootSource.contains("viewModel.bootstrapIfNeeded()"))
        let launchBlock = try functionBlock(
            named: "private var launchExperienceOverlay",
            endingBefore: "private var configuredMainSplitContainer",
            in: rootSource
        )
        XCTAssertFalse(launchBlock.contains("Text(\"Rune\")"))

        XCTAssertTrue(viewModelSource.contains("@Published public private(set) var isLaunchExperienceVisible = true"))
        XCTAssertTrue(viewModelSource.contains("launchExperienceMinimumNanoseconds: UInt64 = 320_000_000"))
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
        let portForwardSource = try String(contentsOfFile: terminalPortForwardPanelViewPath, encoding: .utf8)
        let sessionControlSource = try String(contentsOfFile: terminalSessionControlRowPath, encoding: .utf8)
        let podSelectorSource = try String(contentsOfFile: terminalPodSelectorRowPath, encoding: .utf8)
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
        XCTAssertTrue(tabBarSource.contains("Text(\"\\(number) New Shell\")"))
        XCTAssertTrue(tabBarSource.contains("accessibilityLabel(\"New Shell\")"))
        XCTAssertTrue(tabBarSource.contains(".frame(width: 40, height: 28)"))
        XCTAssertTrue(tabBarSource.contains(".contentShape(Rectangle())"))
        XCTAssertTrue(tabBarSource.contains(".frame(height: 36)"))
        XCTAssertTrue(tabBarSource.contains("tabBorder(isActive: true, isDraft: true)"))
        XCTAssertTrue(tabBarSource.contains("Color.primary.opacity(0.16)"))

        XCTAssertTrue(sessionControlSource.contains("Label(primaryActionTitle, systemImage: primaryActionSystemImage)"))
        XCTAssertTrue(sessionControlSource.contains(".frame(width: 104)"))
        XCTAssertTrue(sessionControlSource.contains("Button(\"Clear\", action: onClear)"))
        XCTAssertTrue(sessionControlSource.contains(".frame(width: 64)"))
        XCTAssertTrue(sessionControlSource.contains("ScrollView(.horizontal, showsIndicators: false)"))
        XCTAssertFalse(sessionControlSource.contains("ViewThatFits(in: .horizontal)"))
        XCTAssertTrue(shellSource.contains("onSaveActiveTranscript"))
        XCTAssertTrue(shellSource.contains("onSaveAllTranscripts"))
        XCTAssertTrue(shellSource.contains("Label(\"Export\", systemImage: \"square.and.arrow.down\")"))
        XCTAssertTrue(shellSource.contains("Button(\"Save Active Transcript\""))
        XCTAssertTrue(shellSource.contains("Button(\"Save All Transcripts ZIP\""))
        XCTAssertTrue(workspaceSource.contains("onSaveActiveTerminalTranscript"))
        XCTAssertTrue(workspaceSource.contains("onSaveAllTerminalTranscripts"))
        XCTAssertTrue(podSelectorSource.contains("ScrollView(.horizontal, showsIndicators: false)"))
        XCTAssertFalse(podSelectorSource.contains("ViewThatFits(in: .horizontal)"))

        XCTAssertTrue(workspaceSource.contains("@State private var isComposingNewShellTab"))
        XCTAssertTrue(workspaceSource.contains("private var isShowingNewShellDraft"))
        XCTAssertTrue(workspaceSource.contains("private func composeNewShellTab()"))

        XCTAssertTrue(workspaceSource.contains("private func terminalHeight(availableHeight: CGFloat, portForwardExpanded: Bool)"))
        XCTAssertFalse(workspaceSource.contains("tabPressure"))
        XCTAssertFalse(workspaceSource.contains("sessionCount"))

        XCTAssertTrue(portForwardSource.contains("compactStatusHeight"))
        XCTAssertTrue(portForwardSource.contains("activeSessionListHeight"))
        XCTAssertTrue(portForwardSource.contains(".frame(height: compactStatusHeight)"))
        XCTAssertTrue(portForwardSource.contains(".frame(height: activeSessionListHeight)"))
        XCTAssertTrue(portForwardSource.contains("stableHorizontalControls"))
        XCTAssertFalse(portForwardSource.contains("ViewThatFits(in: .horizontal)"))

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
        let deploymentOverview = try functionBlock(
            named: "private func deploymentOverviewSection",
            endingBefore: "private func inspectorEmptyState",
            in: source
        )
        guard let actionRowStart = deploymentOverview.range(of: "inspectorActionButtonRow {"),
              let spacer = deploymentOverview.range(
                of: "Spacer(minLength: 0)",
                range: actionRowStart.upperBound..<deploymentOverview.endIndex
              ) else {
            XCTFail("Expected deployment overview action row with trailing spacer")
            return
        }
        let actionRowPrefix = String(deploymentOverview[actionRowStart.lowerBound..<spacer.lowerBound])

        XCTAssertTrue(
            actionRowPrefix.contains("Button(\"Delete\", role: .destructive)"),
            "Delete should stay in the stable horizontal action row, not drop onto its own lower row."
        )
        XCTAssertTrue(deploymentOverview.contains("Button(\"Restart Rollout\")"))
        XCTAssertTrue(deploymentOverview.contains("Button(\"Export Pod YAML ZIP\")"))
    }

    func testServiceOverviewKeepsPrimaryActionsInOneStableActionRow() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let serviceOverview = try functionBlock(
            named: "private var serviceDetails: some View {",
            endingBefore: "private var eventDetails: some View {",
            in: source
        )
        guard let actionRowStart = serviceOverview.range(of: "inspectorActionButtonRow {"),
              let spacer = serviceOverview.range(
                of: "Spacer(minLength: 0)",
                range: actionRowStart.upperBound..<serviceOverview.endIndex
              ) else {
            XCTFail("Expected service overview action row with trailing spacer")
            return
        }
        let actionRowPrefix = String(serviceOverview[actionRowStart.lowerBound..<spacer.lowerBound])

        XCTAssertTrue(
            actionRowPrefix.contains("Button(\"Delete\", role: .destructive)"),
            "Service Delete should stay in the stable horizontal action row, not drop onto its own lower row."
        )
        XCTAssertTrue(serviceOverview.contains("Button(\"Apply YAML\")"))
        XCTAssertTrue(serviceOverview.contains("Button(\"Export…\")"))
    }

    func testResourceFilterEmptyStatesExplainNamespaceAndClearAction() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("resourceFilterEmptyState(kindTitle: \"Pods\", totalCount: viewModel.state.pods.count)"))
        XCTAssertTrue(source.contains("resourceFilterEmptyState(kindTitle: \"Deployments\", totalCount: viewModel.state.deployments.count)"))
        XCTAssertTrue(source.contains("resourceFilterEmptyState(kindTitle: \"Services\", totalCount: viewModel.state.services.count)"))
        XCTAssertTrue(source.contains("Clear Filter"))
        XCTAssertTrue(source.contains("This filter only searches"))
        XCTAssertTrue(source.contains("switch namespace"))
    }

    func testResourceFilterClearButtonIsReservedOutsideTextField() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let controls = try functionBlock(
            named: "private var namespaceAndFilterControls: some View {",
            endingBefore: "@ViewBuilder\n    private var sectionSpecificControls",
            in: source
        )

        XCTAssertTrue(controls.contains("Image(systemName: \"xmark.circle.fill\")"))
        XCTAssertTrue(controls.contains("viewModel.setResourceSearchQuery(\"\")"))
        XCTAssertTrue(controls.contains("textInputFocus = .resourceFilter"))
        XCTAssertTrue(controls.contains(".frame(width: 22, height: RuneUILayoutMetrics.headerChipHeight"))
        XCTAssertTrue(controls.contains(".opacity(viewModel.state.resourceSearchQuery.isEmpty ? 0 : 1)"))
        XCTAssertTrue(controls.contains(".disabled(viewModel.state.resourceSearchQuery.isEmpty)"))
        XCTAssertFalse(controls.contains(".overlay"))
    }

    func testLocalK8sIntegrationReportScriptUsesSandboxSafeSwiftHarness() throws {
        let source = try String(contentsOfFile: localK8sIntegrationReportScriptPath, encoding: .utf8)

        XCTAssertTrue(source.contains("MODULE_CACHE_DIR="))
        XCTAssertTrue(source.contains("CLANG_MODULE_CACHE_PATH"))
        XCTAssertTrue(source.contains("SWIFTPM_MODULECACHE_OVERRIDE"))
        XCTAssertTrue(source.contains("swift build --disable-sandbox --product RuneFakeK8s"))
        XCTAssertTrue(source.contains("swift test --disable-sandbox --filter LocalKubernetesIntegrationTests/testRuneFakeK8sEventsPointAtExistingPods"))
        XCTAssertTrue(source.contains("swift test --disable-sandbox --filter RuneFakeK8sRESTServerTests"))
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
        XCTAssertTrue(source.contains("accessibilityLabel(\"Find in terminal\")"))
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
        XCTAssertTrue(shellSource.contains("private enum TerminalPromptPalette"))
        XCTAssertTrue(shellSource.contains("TerminalPromptPalette.inputTextColor"))
        XCTAssertTrue(shellSource.contains("TerminalPromptPalette.disabledInputTextColor"))
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
        XCTAssertTrue(workspaceSource.contains("availablePods.first(where: { !hasShellSession(for: $0) })"))
        XCTAssertTrue(workspaceSource.contains("private func hasShellSession(for pod: PodSummary) -> Bool"))

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
        XCTAssertTrue(preferencesSource.contains("settingsSection(\"Appearance\")"))
        XCTAssertTrue(preferencesSource.contains("Text(\"Font size\")"))
        XCTAssertTrue(preferencesSource.contains("Slider("))
        XCTAssertTrue(preferencesSource.contains("Button(\"Reset\")"))
        XCTAssertTrue(preferencesSource.contains("terminalFontSize = RuneSettingsKeys.terminalFontSizeDefault"))
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
        XCTAssertTrue(preferencesSource.contains("case safety"))
        XCTAssertTrue(preferencesSource.contains("settingsSection(\"Write safety\")"))
        XCTAssertTrue(preferencesSource.contains("settingsSection(\"Rollback safety\")"))
        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.writeSafetyRequireApplyDryRun)"))
        XCTAssertTrue(rootViewSource.contains("if !viewModel.pendingWriteActionKubectlCommand.isEmpty"))
        XCTAssertTrue(viewModelSource.contains("runeWriteSafetyRequireRolloutDryRun"))
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
        XCTAssertTrue(preferencesSource.contains("Hide managed fields by default"))
        XCTAssertTrue(describeSource.contains("@AppStorage(RuneSettingsKeys.hideManagedFieldsByDefault)"))
        XCTAssertTrue(yamlSource.contains("@AppStorage(RuneSettingsKeys.hideManagedFieldsByDefault)"))
    }

    func testFontSizePreferenceScalesRootInterfaceAndManifestText() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let textViewSource = try String(contentsOfFile: appKitManifestTextViewPath, encoding: .utf8)

        XCTAssertTrue(rootViewSource.contains("@AppStorage(RuneSettingsKeys.terminalFontSize)"))
        XCTAssertTrue(rootViewSource.contains(".dynamicTypeSize(appDynamicTypeSize)"))
        XCTAssertTrue(rootViewSource.contains("private var appDynamicTypeSize: DynamicTypeSize"))
        XCTAssertTrue(textViewSource.contains("@AppStorage(RuneSettingsKeys.terminalFontSize)"))
        XCTAssertTrue(textViewSource.contains("fontSize: clampedFontSize"))
        XCTAssertTrue(textViewSource.contains("NSFont.monospacedSystemFont(ofSize: configuredFontSize"))
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
        XCTAssertTrue(podOverview.contains("inspectorInfoRow(\"Namespace\""))
        XCTAssertTrue(podOverview.contains("podOverviewRow(title: \"Status\""))
        XCTAssertTrue(podOverview.contains("RuneInspectorInfoRow(\"Containers\""))
        XCTAssertFalse(podOverview.contains("inspectorInsetCard"))
        XCTAssertFalse(podOverview.contains(".runeInsetCard()"))

        let deploymentOverview = try functionBlock(
            named: "private func deploymentOverviewSection",
            endingBefore: "private func inspectorEmptyState",
            in: source
        )
        XCTAssertTrue(deploymentOverview.contains("inspectorInfoRow(\"Namespace\""))
        XCTAssertTrue(deploymentOverview.contains("inspectorActionButtonRow"))
        XCTAssertTrue(deploymentOverview.contains("Button(\"Export Pod Logs ZIP\")"))
        XCTAssertTrue(deploymentOverview.contains("viewModel.saveDeploymentPodLogsZip()"))
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
        XCTAssertTrue(serviceDetails.contains("inspectorInfoRow(\"Namespace\""))
        XCTAssertTrue(serviceDetails.contains("inspectorInfoRow(\"Type\""))
        XCTAssertTrue(serviceDetails.contains("inspectorInfoRow(\"Cluster IP\""))
        XCTAssertTrue(serviceDetails.contains("inspectorActionButtonRow"))
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
        XCTAssertTrue(appKitPodTableSource.contains("NSColor.headerTextColor"))
        XCTAssertTrue(appKitPodTableSource.contains("label.centerYAnchor.constraint(equalTo: container.centerYAnchor)"))
        XCTAssertTrue(appKitPodTableSource.contains("drawSortIndicator(indicatorImage"))
        XCTAssertTrue(appKitPodTableSource.contains("resetColumnWidth(_ columnID: String)"))
        XCTAssertTrue(appKitPodTableSource.contains("event.clickCount == 2"))
        XCTAssertTrue(appKitPodTableSource.contains("drawColumnDivider"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneUILayoutMetrics.compactGlyphCornerRadius"))
        XCTAssertTrue(appKitPodTableSource.contains("NSColor.controlBackgroundColor.withAlphaComponent(0.42)"))
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
        XCTAssertTrue(appKitPodTableSource.contains("headerView.resizableColumnIdentifiers = Set("))
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
        XCTAssertTrue(appKitPodTableSource.contains("static let rowHeight: CGFloat = 34"))
        XCTAssertTrue(appKitPodTableSource.contains("static let rowGap: CGFloat = 4"))
        XCTAssertTrue(appKitPodTableSource.contains("static let rowHorizontalInset: CGFloat = 6"))
        XCTAssertTrue(appKitPodTableSource.contains("static let contentLeadingInset: CGFloat = 10"))
        XCTAssertTrue(appKitPodTableSource.contains("static let contentTrailingInset: CGFloat = 10"))
        XCTAssertTrue(appKitPodTableSource.contains("static let sortIndicatorGap: CGFloat = 4"))
        XCTAssertTrue(appKitPodTableSource.contains("headerView.horizontalInset = rowHorizontalInset"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceTableRowView(horizontalInset: RuneAppKitResourceTableStyle.rowHorizontalInset)"))
        XCTAssertEqual(appKitPodTableSource.components(separatedBy: "RuneAppKitResourceTableStyle.apply(to: tableView").count - 1, 7)
        XCTAssertFalse(appKitPodTableSource.contains("usesAlternatingRowBackgroundColors = false\n        tableView.backgroundColor = .clear\n        tableView.gridStyleMask = []\n        tableView.headerView"))
        XCTAssertTrue(appKitPodTableSource.contains("lineBreakMode: NSLineBreakMode = .byTruncatingTail"))
        XCTAssertTrue(appKitPodTableSource.contains("titleLabel.lineBreakMode = .byTruncatingMiddle"))
    }

    func testPodNameColumnWidthIsClampedToUsableRange() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let layout = try functionBlock(
            named: "enum PodTableLayout {",
            endingBefore: "private enum RuneRootKeyboardPane",
            in: rootViewSource
        )

        XCTAssertTrue(layout.contains("nameColumnMinimumWidth"))
        XCTAssertTrue(layout.contains("nameColumnMaximumWidth"))
        XCTAssertTrue(layout.contains("clampedNameColumnWidth"))
        XCTAssertTrue(layout.contains("width.rounded(.toNearestOrAwayFromZero)"))
        XCTAssertTrue(layout.contains("min(nameColumnMaximumWidth, max(nameColumnMinimumWidth, pixelAlignedWidth))"))
        XCTAssertTrue(layout.contains("restartsWidth: CGFloat = 82"))
        XCTAssertTrue(layout.contains("favoriteColumnWidth: CGFloat = 34"))
        XCTAssertTrue(layout.contains("defaultAppKitTableWidth"))
        XCTAssertTrue(layout.contains("+ nameColumnDefaultWidth"))
    }

    func testPodNameColumnHeaderAndRowsShareSelectionGutterForResizeAlignment() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let appKitPodTableSource = try String(contentsOfFile: appKitPodTableViewPath, encoding: .utf8)
        let layout = try functionBlock(
            named: "enum PodTableLayout {",
            endingBefore: "private enum RuneRootKeyboardPane",
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
        XCTAssertTrue(appKitPodTableSource.contains("return favoriteCell(isFavorite: parent.isFavorite(pod), row: row)"))
        XCTAssertTrue(appKitPodTableSource.contains("@objc private func toggleFavoriteButton"))
        XCTAssertTrue(appKitPodTableSource.contains("static let rowGap: CGFloat = 4"))
        XCTAssertTrue(appKitPodTableSource.contains("tableView.intercellSpacing = NSSize(width: 0, height: rowGap)"))
        XCTAssertTrue(appKitPodTableSource.contains("x: bounds.minX + horizontalInset"))
        XCTAssertFalse(appKitPodTableSource.contains("drawInterior(withFrame: cellFrame.insetBy(dx: 4, dy: 0)"))
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
        XCTAssertTrue(source.contains("LazyVStack(alignment: .leading, spacing: 8)"))
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
        XCTAssertTrue(portForwardPanelSource.contains("if session.isActiveOrStarting"))
        XCTAssertTrue(portForwardPanelSource.contains("Label(\"Open in Browser\", systemImage: \"safari\")"))
        XCTAssertTrue(portForwardPanelSource.contains("onOpenPortForwardInBrowser(session)"))
        XCTAssertTrue(portForwardPanelSource.contains("onStopPortForward(session)"))
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
        XCTAssertTrue(designSource.contains("struct RuneSelectionCheckboxButton"))
        XCTAssertTrue(designSource.contains(".toggleStyle(.checkbox)"))
        XCTAssertTrue(designSource.contains("allVisibleSelected ? \"Deselect All\" : \"Select All\""))
    }

    func testGlobalErrorsRenderAsStructuredNoticesNotLooseRedFooterText() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let designSource = try String(contentsOfFile: runeDesignComponentsPath, encoding: .utf8)

        guard let detailPaneStart = rootViewSource.range(of: "private var detailPane: some View {"),
              let overviewDetailsStart = rootViewSource.range(of: "private var overviewDetails: some View {", range: detailPaneStart.upperBound..<rootViewSource.endIndex)
        else {
            XCTFail("Could not locate detail pane block")
            return
        }

        let detailPaneBlock = String(rootViewSource[detailPaneStart.lowerBound..<overviewDetailsStart.lowerBound])
        XCTAssertTrue(detailPaneBlock.contains("RuneNoticeBanner(notice: notice)"))
        XCTAssertTrue(detailPaneBlock.contains("viewModel.state.activeNotice"))
        XCTAssertFalse(detailPaneBlock.contains("Text(error)"))
        XCTAssertFalse(detailPaneBlock.contains(".foregroundStyle(.red)"))
        XCTAssertTrue(designSource.contains("struct RuneNoticeBanner"))
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
        let pickerSource = try String(contentsOfFile: kubeConfigPickerPath, encoding: .utf8)
        let kubernetesClientSource = try String(contentsOfFile: kubernetesClientPath, encoding: .utf8)

        XCTAssertTrue(rootViewSource.contains("Add Cluster"))
        XCTAssertTrue(rootViewSource.contains("RuneGlassPaneSurface(role: .content)"))
        XCTAssertTrue(rootViewSource.contains("RuneSurfaceBackground(kind: .inset)"))
        XCTAssertTrue(rootViewSource.contains("RuneUILayoutMetrics.paneShellCornerRadius"))
        XCTAssertTrue(rootViewSource.contains(".fixedSize(horizontal: false, vertical: true)"))
        XCTAssertTrue(rootViewSource.contains(".id(addClusterPopoverLayoutID)"))
        XCTAssertTrue(rootViewSource.contains("private var addClusterPopoverLayoutID: String"))
        XCTAssertTrue(rootViewSource.contains("isManualAddClusterExpanded = false"))
        XCTAssertTrue(rootViewSource.contains("Standard"))
        XCTAssertTrue(rootViewSource.contains("addClusterDiscoveryStatus"))
        XCTAssertTrue(rootViewSource.contains("Auto-detect Clusters"))
        XCTAssertTrue(rootViewSource.contains("On - click to refresh detected contexts"))
        XCTAssertTrue(rootViewSource.contains("Reading cluster contexts..."))
        XCTAssertTrue(rootViewSource.contains("Watching for kubeconfig sources"))
        XCTAssertTrue(rootViewSource.contains("Use Default"))
        XCTAssertTrue(rootViewSource.contains("~/.kube/config"))
        XCTAssertTrue(rootViewSource.contains("Refresh detected contexts"))
        XCTAssertTrue(rootViewSource.contains("Import Kubeconfig…"))
        XCTAssertTrue(rootViewSource.contains("Paste"))
        XCTAssertTrue(rootViewSource.contains("Import Options"))
        XCTAssertTrue(rootViewSource.contains("Import File"))
        XCTAssertTrue(rootViewSource.contains("Paste Kubeconfig"))
        XCTAssertTrue(rootViewSource.contains("viewModel.importKubeConfigFromPasteboard()"))
        XCTAssertTrue(rootViewSource.contains("Import Folder…"))
        XCTAssertTrue(rootViewSource.contains("viewModel.importKubeConfigFolder()"))
        XCTAssertTrue(rootViewSource.contains("Favorite imported contexts"))
        XCTAssertTrue(rootViewSource.contains("$viewModel.favoriteImportedKubeConfigContexts"))
        XCTAssertTrue(rootViewSource.contains("Advanced"))
        XCTAssertTrue(rootViewSource.contains("Provider Login"))
        XCTAssertTrue(rootViewSource.contains("Manual Token Server"))
        XCTAssertTrue(rootViewSource.contains("Add Manual Token Cluster"))
        XCTAssertTrue(rootViewSource.contains("viewModel.importManualTokenKubeConfig()"))
        XCTAssertTrue(rootViewSource.contains("KubeConfigImportReviewPanel"))
        XCTAssertTrue(rootViewSource.contains("viewModel.clearKubeConfigImportReviews"))
        XCTAssertTrue(importReviewPanelSource.contains("Text(\"Import Review\")"))
        XCTAssertTrue(importReviewPanelSource.contains("Label(\"Clear\", systemImage: \"xmark.circle\")"))
        XCTAssertTrue(importReviewPanelSource.contains("doc.text.magnifyingglass"))
        XCTAssertTrue(importReviewPanelSource.contains("Text(\"Full review\")"))
        XCTAssertTrue(importReviewPanelSource.contains("Text(\"Contexts"))
        XCTAssertTrue(importReviewPanelSource.contains("Text(\"Issues"))
        XCTAssertTrue(importReviewPanelSource.contains("Text(\"Redacted kubeconfig\")"))
        XCTAssertTrue(importReviewPanelSource.contains("Run Auth Doctor"))
        XCTAssertTrue(importReviewPanelSource.contains("update existing, import as copy, or skip"))
        XCTAssertTrue(rootViewSource.contains("Microsoft AKS"))
        XCTAssertTrue(rootViewSource.contains("Amazon EKS"))
        XCTAssertTrue(rootViewSource.contains("Google GKE"))
        XCTAssertTrue(rootViewSource.contains("Local Cluster"))
        XCTAssertTrue(rootViewSource.contains("az aks get-credentials"))
        XCTAssertTrue(rootViewSource.contains("aws eks update-kubeconfig"))
        XCTAssertTrue(rootViewSource.contains("gcloud container clusters get-credentials"))
        XCTAssertTrue(rootViewSource.contains("--location <location>"))
        XCTAssertTrue(rootViewSource.contains("Refresh Contexts"))
        XCTAssertTrue(rootViewSource.contains("viewModel.refreshKubeConfigSourcesFromDiscovery()"))
        XCTAssertTrue(rootViewSource.contains("Run & Connect"))
        XCTAssertTrue(rootViewSource.contains("viewModel.runCloudKubeConfigImport"))
        XCTAssertTrue(rootViewSource.contains("Label(\"Auth Doctor\", systemImage: \"stethoscope\")"))
        XCTAssertTrue(rootViewSource.contains("Command Details"))
        XCTAssertTrue(rootViewSource.contains("TextField(\"Cluster name\""))
        XCTAssertTrue(rootViewSource.contains("Role ARN (optional)"))
        XCTAssertTrue(rootViewSource.contains("Subscription ID or name (optional)"))
        XCTAssertTrue(rootViewSource.contains("Project ID"))
        XCTAssertTrue(rootViewSource.contains("kind, k3s, k3d, minikube"))
        XCTAssertTrue(rootViewSource.contains("kind get clusters && minikube status && k3d cluster list && k3s kubectl config current-context"))
        XCTAssertTrue(rootViewSource.contains("sudo cat /etc/rancher/k3s/k3s.yaml"))
        XCTAssertTrue(rootViewSource.contains("k3d kubeconfig get <cluster-name>"))
        XCTAssertTrue(rootViewSource.contains("kind get kubeconfig --name <cluster-name>"))
        XCTAssertTrue(rootViewSource.contains("minikube start"))
        XCTAssertTrue(rootViewSource.contains("minikube stop"))
        XCTAssertTrue(viewModelSource.contains("func addDefaultKubeConfig()"))
        XCTAssertTrue(viewModelSource.contains("func syncKubeConfigSourcesFromDiscovery(reason: String)"))
        XCTAssertTrue(viewModelSource.contains("func runCloudKubeConfigImport"))
        XCTAssertTrue(viewModelSource.contains("self.runAuthDoctor()"))
        XCTAssertTrue(viewModelSource.contains("cloud-login-\\(request.provider.rawValue)"))
        XCTAssertTrue(viewModelSource.contains("CloudKubeConfigImporting"))
        XCTAssertTrue(viewModelSource.contains("kubeConfigSourceSyncNanoseconds"))
        XCTAssertTrue(viewModelSource.contains("state.resourceYAMLHasUnsavedEdits"))
        XCTAssertTrue(viewModelSource.contains("APP_SANDBOX_CONTAINER_ID"))
        XCTAssertTrue(viewModelSource.contains("pickDefaultKubeConfig(at: url)"))
        XCTAssertTrue(viewModelSource.contains("getpwuid(getuid())"))
        XCTAssertTrue(viewModelSource.contains("merged[standardizedPath] = source"))
        XCTAssertTrue(pickerSource.contains("func pickDefaultKubeConfig(at defaultURL: URL) throws -> URL?"))
        XCTAssertTrue(pickerSource.contains("panel.showsHiddenFiles = true"))
        XCTAssertFalse(pickerSource.contains("allowedContentTypes"))
        XCTAssertTrue(kubernetesClientSource.contains("access.retainAccess(to: url)"))
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
        XCTAssertTrue(authDoctorBlock.contains("authDoctorSummaryChip"))
        XCTAssertTrue(authDoctorBlock.contains("shouldReserveAuthDoctorPanel"))
        XCTAssertTrue(authDoctorBlock.contains("\"Run Auth Doctor\""))
        XCTAssertTrue(authDoctorBlock.contains("\"Ready\""))
        XCTAssertTrue(authDoctorBlock.contains("Button(\"Save Bundle\")"))
        XCTAssertTrue(authDoctorBlock.contains("viewModel.saveSupportBundle()"))
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
        XCTAssertTrue(authDoctorBlock.contains("viewModel.reviewLoadedKubeConfigSources()"))
        XCTAssertTrue(authDoctorBlock.contains("addClusterPopoverPresented = true"))
        XCTAssertTrue(authDoctorBlock.contains("NSWorkspace.shared.open(url)"))

        let authDoctorVisibilityBlock = try functionBlock(
            named: "private var shouldReserveAuthDoctorPanel: Bool",
            endingBefore: "@ViewBuilder\n    private var authDoctorSummaryChip",
            in: rootViewSource
        )
        XCTAssertTrue(authDoctorVisibilityBlock.contains("case .overview"))
        XCTAssertFalse(authDoctorVisibilityBlock.contains("case .overview, .workloads"))

        let workloadsBlock = try functionBlock(
            named: "private var workloadsPane: some View",
            endingBefore: "private var networkingPane",
            in: rootViewSource
        )
        XCTAssertFalse(workloadsBlock.contains("authDoctorPanel"))
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
        XCTAssertTrue(authDoctorBlock.contains("listPods"))
        XCTAssertTrue(authDoctorBlock.contains("podLogs"))
        XCTAssertFalse(authDoctorBlock.contains("applyYAML"))
        XCTAssertFalse(authDoctorBlock.contains("delete"))
        XCTAssertFalse(authDoctorBlock.contains("rollback"))
        XCTAssertFalse(authDoctorBlock.contains("execInPod"))
        XCTAssertFalse(authDoctorBlock.contains("helmCommandRunner"))
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

        XCTAssertTrue(logsViewSource.contains("Toggle(\"Tail\""))
        XCTAssertTrue(logsViewSource.contains("Button(isStreamPaused ? \"Resume\" : \"Pause\""))
        XCTAssertTrue(logsViewSource.contains("contentStyle: .ansiLogs"))
        XCTAssertTrue(logsViewSource.contains("Label(\"Save Logs\", systemImage: \"square.and.arrow.down\")"))
        XCTAssertTrue(logsViewSource.contains("Label(\"More\", systemImage: \"ellipsis.circle\")"))
        XCTAssertTrue(logsViewSource.contains("Label(\"Export Visible Results ZIP\", systemImage: \"doc.zipper\")"))
        XCTAssertTrue(logsViewSource.contains("Label(\"Export Full Unfiltered ZIP\", systemImage: \"archivebox\")"))
        XCTAssertTrue(logsViewSource.contains("Label(\"Export All Pods Full ZIP\", systemImage: \"shippingbox\")"))
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
        XCTAssertTrue(appKitPodTableSource.contains("resourceMaximumContentWidth: CGFloat = 1_200"))
        XCTAssertTrue(appKitPodTableSource.contains("deploymentMaximumContentWidth = resourceMaximumContentWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceListLayout.deploymentColumnWidths(visibleWidth: visibleWidth)"))
        XCTAssertTrue(appKitPodTableSource.contains("tableView.enclosingScrollView?.contentView.bounds.width"))
        XCTAssertTrue(appKitPodTableSource.contains("tableView.setFrameSize(NSSize(width: tableWidth, height: tableView.frame.height))"))
        XCTAssertTrue(appKitPodTableSource.contains("applyImmediateResourceContextMenuSelection"))
        XCTAssertTrue(appKitPodTableSource.contains("coordinator?.selectRowForContextMenu(row, in: self)"))
        XCTAssertTrue(appKitPodTableSource.contains("DispatchQueue.main.async { [weak self]"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceListScrollView"))
        XCTAssertTrue(appKitPodTableSource.contains("onVisibleWidthChanged"))
        XCTAssertTrue(appKitPodTableSource.contains("override func layout()"))
        XCTAssertTrue(appKitPodTableSource.contains("coordinator?.updateColumnWidths()"))
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
        XCTAssertTrue(rootViewSource.contains("private enum HelmBrowserTab"))
        XCTAssertTrue(rootViewSource.contains("@State private var helmBrowserTab: HelmBrowserTab = .releases"))
        XCTAssertTrue(rootViewSource.contains("RuneSegmentedPickerInScroll("))
        XCTAssertTrue(rootViewSource.contains("selection: $helmBrowserTab"))
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

    func testGenericResourceBulkSelectionExposesQuickCompareAction() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)

        XCTAssertTrue(rootViewSource.contains("Label(\"Compare\", systemImage: \"rectangle.split.2x1\")"))
        XCTAssertTrue(rootViewSource.contains("viewModel.copySelectedGenericResourceComparisonToClipboard()"))
        XCTAssertTrue(rootViewSource.contains("viewModel.canCopySelectedGenericResourceComparison"))
        XCTAssertTrue(viewModelSource.contains("selectedGenericResourceComparisonText"))
        XCTAssertTrue(viewModelSource.contains("copySelectedGenericResourceComparisonToClipboard"))
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
        XCTAssertTrue(rootViewSource.contains("case 123:"))
        XCTAssertTrue(rootViewSource.contains("return \"left\""))
        XCTAssertTrue(rootViewSource.contains("case 124:"))
        XCTAssertTrue(rootViewSource.contains("return \"right\""))
        XCTAssertTrue(rootViewSource.contains("let disallowedModifiers: NSEvent.ModifierFlags = [.function]"))
    }

    func testPreferencesExposeArrowKeysForHistoryBindings() throws {
        let testFile = URL(fileURLWithPath: #filePath)
        let root = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let preferencesPath = root.appendingPathComponent("Sources/RuneUI/Views/RunePreferencesView.swift").path
        let preferencesSource = try String(contentsOfFile: preferencesPath, encoding: .utf8)

        XCTAssertTrue(preferencesSource.contains("[\"[\", \"]\", \"/\", \":\", \"?\", \"left\", \"right\"]"))
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
        XCTAssertTrue(confirmBlock.contains("isProductionContext"))
        XCTAssertTrue(confirmBlock.contains("action.isDestructive"))
        XCTAssertTrue(confirmBlock.contains("runeWriteSafetyRequireProductionSecondConfirmation"))
        XCTAssertTrue(confirmBlock.contains("pendingProductionDestructiveConfirmation != action"))
        XCTAssertTrue(confirmBlock.contains("pendingProductionDestructiveConfirmation = action"))
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
        XCTAssertTrue(rootViewSource.contains("Production context detected"))
        XCTAssertTrue(rootViewSource.contains("Marked as production"))
    }

    func testCustomResourceBrowserUsesPaginationAndPinnedColumns() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let kubeClientSource = try String(contentsOfFile: kubernetesClientPath, encoding: .utf8)

        XCTAssertTrue(viewModelSource.contains("private static let operatorResourcePageSize = 40"))
        XCTAssertTrue(viewModelSource.contains("public var pagedOperatorResources"))
        XCTAssertTrue(viewModelSource.contains("OperatorResourceListSortColumn"))
        XCTAssertTrue(viewModelSource.contains("toggleOperatorResourceSort"))
        XCTAssertTrue(rootViewSource.contains("AppKitOperatorResourceListView("))
        XCTAssertTrue(rootViewSource.contains("resources: viewModel.pagedOperatorResources"))
        XCTAssertTrue(rootViewSource.contains("sortColumn: viewModel.operatorResourceSortColumn"))
        XCTAssertTrue(rootViewSource.contains("onToggleSort: viewModel.toggleOperatorResourceSort"))
        XCTAssertTrue(kubeClientSource.contains(".prefix(48)"))
        XCTAssertTrue(kubeClientSource.contains(".prefix(16)"))
        XCTAssertTrue(kubeClientSource.contains("if output.count >= 500 { return output }"))
    }

    private var runeRootViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift").path
    }

    private var kubeConfigImportReviewPanelPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/KubeConfigImportReviewPanel.swift").path
    }

    private var appKitPodTableViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/AppKitPodTableView.swift").path
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
}
