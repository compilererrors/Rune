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
                    onStartSession: { _ in },
                    onReconnectSession: { _, _ in },
                    onStartPortForward: { _ in },
                    onStopPortForward: { _ in },
                    onOpenPortForwardInBrowser: { _ in },
                    onSend: {},
                    onDisconnect: {},
                    onSelectSession: { _ in },
                    onCloseSession: { _ in },
                    onClearTranscript: {}
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

    func testTerminalWorkspaceComposesIndependentPanelsInStableOrder() throws {
        let workspaceSource = try String(contentsOfFile: resourceTerminalInspectorViewPath, encoding: .utf8)
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
        XCTAssertTrue(tabBarSource.contains("ScrollView(.horizontal"))
        XCTAssertTrue(tabBarSource.contains("accessibilityLabel(\"New Shell\")"))
        XCTAssertTrue(sessionControlSource.contains("struct TerminalSessionControlRow"))
        XCTAssertTrue(transcriptSource.contains("struct TerminalTranscriptSurface"))
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
        XCTAssertTrue(rootSource.contains("Bundle.module.url(forResource: \"rune_logo_main\", withExtension: \"png\")"))
        XCTAssertTrue(rootSource.contains("Bundle.main.url(forResource: \"rune_logo_main\", withExtension: \"png\")"))
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

    func testTerminalControlsAreScopedToActiveTabAndReserveStableSpace() throws {
        let workspaceSource = try String(contentsOfFile: resourceTerminalInspectorViewPath, encoding: .utf8)
        let shellSource = try String(contentsOfFile: terminalShellPanelViewPath, encoding: .utf8)
        let tabBarSource = try String(contentsOfFile: terminalSessionTabBarPath, encoding: .utf8)
        let portForwardSource = try String(contentsOfFile: terminalPortForwardPanelViewPath, encoding: .utf8)
        let sessionControlSource = try String(contentsOfFile: terminalSessionControlRowPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)

        XCTAssertTrue(shellSource.contains("private var primaryActionTitle"))
        XCTAssertTrue(shellSource.contains("return \"Disconnect\""))
        XCTAssertTrue(shellSource.contains("return \"Reconnect\""))
        XCTAssertTrue(shellSource.contains("private var primaryActionDisabled"))
        XCTAssertTrue(shellSource.contains("private var primaryActionSystemImage"))
        XCTAssertTrue(shellSource.contains("onReconnectSession(session, selectedPod)"))
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

        XCTAssertTrue(viewModelSource.contains("replacingSessionID: String? = nil"))
        XCTAssertTrue(viewModelSource.contains("guard session.status == .connected else { return }"))
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

    func testTerminalAndPortForwardLifetimeStopsBackingSessions() throws {
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let kubeClientSource = try String(contentsOfFile: kubernetesClientPath, encoding: .utf8)

        XCTAssertTrue(viewModelSource.contains("public func closeTerminalSession(id: String)"))
        XCTAssertTrue(viewModelSource.contains("state.selectTerminalSession(id: id)"))
        XCTAssertTrue(viewModelSource.contains("stopTerminalSession(resetState: true)"))
        XCTAssertTrue(viewModelSource.contains("await kubeClient.stopPodTerminalSession(id: sessionID)"))
        XCTAssertTrue(viewModelSource.contains("public func stopPortForward(_ session: PortForwardSession)"))
        XCTAssertTrue(viewModelSource.contains("await kubeClient.stopPortForward(sessionID: session.id)"))
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
        XCTAssertFalse(deploymentOverview.contains("inspectorInsetCard"))
        XCTAssertFalse(deploymentOverview.contains(".runeInsetCard()"))

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
        XCTAssertTrue(portForwardPanelSource.contains("if session.status == .active, session.browserURL != nil"))
        XCTAssertTrue(portForwardPanelSource.contains("if session.status == .starting || session.status == .active || session.status == .failed"))
        XCTAssertTrue(portForwardPanelSource.contains("Label(\"Open in Browser\", systemImage: \"safari\")"))
        XCTAssertTrue(portForwardPanelSource.contains("onOpenPortForwardInBrowser(session)"))
        XCTAssertTrue(portForwardPanelSource.contains("onStopPortForward(session)"))
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

        XCTAssertTrue(rootViewSource.contains("Add Cluster"))
        XCTAssertTrue(rootViewSource.contains("Import Kubeconfig"))
        XCTAssertTrue(rootViewSource.contains("Use ~/.kube/config"))
        XCTAssertTrue(rootViewSource.contains("Microsoft AKS"))
        XCTAssertTrue(rootViewSource.contains("Amazon EKS"))
        XCTAssertTrue(rootViewSource.contains("Google GKE"))
        XCTAssertTrue(rootViewSource.contains("Local Cluster"))
        XCTAssertTrue(rootViewSource.contains("az aks get-credentials"))
        XCTAssertTrue(rootViewSource.contains("aws eks update-kubeconfig"))
        XCTAssertTrue(rootViewSource.contains("gcloud container clusters get-credentials"))
        XCTAssertTrue(viewModelSource.contains("func addDefaultKubeConfig()"))
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
              let namespaceWarning = viewModelSource.range(of: "warnings.append(\"namespaces: \\(error.localizedDescription)\")")
        else {
            XCTFail("Namespace snapshot cancellation handling was not found")
            return
        }
        XCTAssertLessThan(namespaceFailure.lowerBound, namespaceWarning.lowerBound)

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
        XCTAssertTrue(logsViewSource.contains("DeferredResourceLogsTextView"))
        XCTAssertTrue(logsViewSource.contains("deferredOutputThreshold"))
        XCTAssertTrue(logsViewSource.contains("renderTask?.cancel()"))
        XCTAssertTrue(viewModelSource.contains("isLogTailModeEnabled"))
        XCTAssertTrue(viewModelSource.contains("tailLogsReloadNanoseconds"))
        XCTAssertTrue(stateSource.contains("sessionLogCache"))
        XCTAssertTrue(stateSource.contains("appendPodLogRead"))
        XCTAssertTrue(stateSource.contains("appendUnifiedServiceLogRead"))
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

        XCTAssertTrue(rootViewSource.contains("podResourceContextMenu(pod)"))
        XCTAssertTrue(rootViewSource.contains("deploymentResourceContextMenu(deployment)"))
        XCTAssertTrue(rootViewSource.contains("serviceResourceContextMenu(service)"))
        XCTAssertTrue(rootViewSource.contains("genericResourceContextMenu(resource, action: action)"))
        XCTAssertTrue(rootViewSource.contains("Open Logs"))
        XCTAssertTrue(rootViewSource.contains("Open Unified Logs"))
        XCTAssertTrue(rootViewSource.contains("Open YAML"))
        XCTAssertTrue(rootViewSource.contains("Describe"))
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
        XCTAssertTrue(keyBindingsSource.contains("RuneKeyboardShortcut(key: \":\", requiresShift: false)"))
        XCTAssertTrue(keyBindingsSource.contains("RuneKeyboardShortcut(key: \"/\", requiresShift: false)"))
        XCTAssertTrue(keyBindingsSource.contains("RuneKeyboardShortcut(key: \"e\", requiresShift: false)"))
        XCTAssertTrue(keyBindingsSource.contains("RuneKeyboardShortcut(key: \"d\", requiresShift: false, requiresControl: true)"))
        XCTAssertTrue(rootViewSource.contains("viewModel.presentCommandPalette()"))
        XCTAssertTrue(rootViewSource.contains("return focusResourceFilterFromKeyBinding()"))
        XCTAssertTrue(rootViewSource.contains("return openYAMLEditorForSelection()"))
        XCTAssertTrue(rootViewSource.contains("return deleteSelectionFromKeyBinding()"))
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

    private var runeRootViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift").path
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

    private var terminalTranscriptSurfacePath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalTranscriptSurface.swift").path
    }

    private var kubernetesClientPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneKube/KubernetesClient.swift").path
    }

    private var runeGlassShellPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Layout/RuneGlassShell.swift").path
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

    private var runeAppStatePath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneCore/State/RuneAppState.swift").path
    }
}
