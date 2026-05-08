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
        XCTAssertTrue(source.contains("can_bind_loopback_socket"))
        XCTAssertTrue(source.contains("Skipped because this environment cannot bind local loopback sockets."))
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
        let settingsSource = try String(contentsOfFile: runeSettingsKeysPath, encoding: .utf8)

        XCTAssertTrue(settingsSource.contains("layoutPodNameColumnWidth"))
        XCTAssertTrue(rootViewSource.contains("@AppStorage(RuneSettingsKeys.layoutPodNameColumnWidth)"))
        XCTAssertTrue(rootViewSource.contains("podNameColumnWidth"))
        XCTAssertTrue(rootViewSource.contains("livePodNameColumnWidth"))
        XCTAssertTrue(rootViewSource.contains("podNameColumnCommittedWidth"))
        XCTAssertTrue(rootViewSource.contains("setLivePodNameColumnWidth"))
        XCTAssertTrue(rootViewSource.contains("commitPodNameColumnWidth"))
        XCTAssertTrue(rootViewSource.contains("podNameColumnResizeHandle"))
        XCTAssertTrue(rootViewSource.contains("DragGesture(minimumDistance: 1)"))
        XCTAssertTrue(rootViewSource.contains("PodTableLayout.minimumScrollableWidth(nameColumnWidth: podNameColumnWidth)"))
        XCTAssertTrue(rootViewSource.contains(".frame(width: podNameColumnWidth, alignment: .leading)"))
        XCTAssertTrue(rootViewSource.contains(".frame(width: podNameColumnWidth + PodTableLayout.nameColumnResizeHandleWidth"))
        XCTAssertTrue(rootViewSource.contains(".onHover { isHovering in"))
        XCTAssertTrue(rootViewSource.contains("NSCursor.resizeLeftRight.push()"))
        XCTAssertTrue(rootViewSource.contains("NSCursor.pop()"))
    }

    func testPodNameColumnWidthIsClampedToUsableRange() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let layout = try functionBlock(
            named: "private enum PodTableLayout {",
            endingBefore: "private enum RuneRootKeyboardPane",
            in: rootViewSource
        )

        XCTAssertTrue(layout.contains("nameColumnMinimumWidth"))
        XCTAssertTrue(layout.contains("nameColumnMaximumWidth"))
        XCTAssertTrue(layout.contains("clampedNameColumnWidth"))
        XCTAssertTrue(layout.contains("min(nameColumnMaximumWidth, max(nameColumnMinimumWidth, width))"))
    }

    func testPodNameColumnHeaderAndRowsShareSelectionGutterForResizeAlignment() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let layout = try functionBlock(
            named: "private enum PodTableLayout {",
            endingBefore: "private enum RuneRootKeyboardPane",
            in: rootViewSource
        )
        let header = try functionBlock(
            named: "private var podTableHeader: some View",
            endingBefore: "private var podNameColumnResizeHandle",
            in: rootViewSource
        )

        XCTAssertTrue(layout.contains("selectionColumnWidth"))
        XCTAssertTrue(layout.contains("+ selectionColumnWidth"))
        XCTAssertTrue(header.contains("Color.clear"))
        XCTAssertTrue(header.contains(".frame(width: PodTableLayout.selectionColumnWidth"))
        XCTAssertTrue(header.contains("podNameColumnResizeHandle"))
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

        XCTAssertTrue(rootViewSource.contains("RuneBulkSelectionBar("))
        XCTAssertTrue(rootViewSource.contains("RuneSelectionCheckboxButton("))
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
        XCTAssertTrue(logsViewSource.contains("usesLargeTextSurface: true"))
        XCTAssertTrue(logsViewSource.contains("largeTextScrollTargetLine"))
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
        XCTAssertTrue(rootViewSource.contains("viewModel.presentCommandPalette()"))
        XCTAssertTrue(rootViewSource.contains("return focusResourceFilterFromKeyBinding()"))
        XCTAssertTrue(rootViewSource.contains("return openYAMLEditorForSelection()"))
        XCTAssertTrue(rootViewSource.contains("return saveCurrentLogsFromKeyBinding()"))
        XCTAssertTrue(rootViewSource.contains("return deleteSelectionFromKeyBinding()"))
    }

    func testSaveLogsShortcutIsScopedToFocusedLogInspector() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let saveLogsBlock = try functionBlock(
            named: "private func saveCurrentLogsFromKeyBinding() -> Bool",
            endingBefore: "private func openShellOrScaleInspectorForSelection",
            in: rootViewSource
        )

        XCTAssertTrue(saveLogsBlock.contains("guard keyboardPaneFocus == .detail else { return false }"))
        XCTAssertTrue(saveLogsBlock.contains("podInspectorTab == .logs"))
        XCTAssertTrue(saveLogsBlock.contains("deploymentInspectorTab == .unifiedLogs"))
        XCTAssertTrue(saveLogsBlock.contains("serviceInspectorTab == .unifiedLogs"))
        XCTAssertTrue(saveLogsBlock.contains("viewModel.saveCurrentLogs()"))
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
        XCTAssertTrue(confirmBlock.contains("pendingProductionDestructiveConfirmation != action"))
        XCTAssertTrue(confirmBlock.contains("pendingProductionDestructiveConfirmation = action"))
    }

    func testCustomResourceBrowserUsesPaginationAndPinnedColumns() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let kubeClientSource = try String(contentsOfFile: kubernetesClientPath, encoding: .utf8)

        XCTAssertTrue(viewModelSource.contains("private static let operatorResourcePageSize = 40"))
        XCTAssertTrue(viewModelSource.contains("public var pagedOperatorResources"))
        XCTAssertTrue(rootViewSource.contains("ForEach(viewModel.pagedOperatorResources)"))
        XCTAssertTrue(rootViewSource.contains("operatorPinnedColumn(resource.namespace ?? \"Cluster\""))
        XCTAssertTrue(rootViewSource.contains("operatorPinnedColumn(resource.apiPath"))
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

    private var runeAppStatePath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneCore/State/RuneAppState.swift").path
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
