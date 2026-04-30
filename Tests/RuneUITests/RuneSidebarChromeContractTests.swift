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

        XCTAssertTrue(rootViewSource.contains("if session.status == .active, session.browserURL != nil"))
        XCTAssertTrue(rootViewSource.contains("Label(\"Open in Browser\", systemImage: \"safari\")"))
        XCTAssertTrue(rootViewSource.contains("viewModel.openPortForwardInBrowser(session)"))
        XCTAssertTrue(rootViewSource.contains("onOpenPortForwardInBrowser: { session in"))
        XCTAssertTrue(rootViewSource.contains("onStopPortForward: { session in"))
        XCTAssertTrue(rootViewSource.contains("viewModel.stopPortForward(session)"))

        XCTAssertTrue(terminalViewSource.contains("let onOpenPortForwardInBrowser: (PortForwardSession) -> Void"))
        XCTAssertTrue(terminalViewSource.contains("let onStopPortForward: (PortForwardSession) -> Void"))
        XCTAssertTrue(terminalViewSource.contains("if session.status == .active, session.browserURL != nil"))
        XCTAssertTrue(terminalViewSource.contains("if session.status == .starting || session.status == .active || session.status == .failed"))
        XCTAssertTrue(terminalViewSource.contains("Label(\"Open in Browser\", systemImage: \"safari\")"))
        XCTAssertTrue(terminalViewSource.contains("onOpenPortForwardInBrowser(session)"))
        XCTAssertTrue(terminalViewSource.contains("onStopPortForward(session)"))
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

    func testLogsExposeTailModeAndSessionCache() throws {
        let logsViewSource = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let stateSource = try String(contentsOfFile: runeAppStatePath, encoding: .utf8)

        XCTAssertTrue(logsViewSource.contains("Toggle(\"Tail\""))
        XCTAssertTrue(viewModelSource.contains("isLogTailModeEnabled"))
        XCTAssertTrue(viewModelSource.contains("tailLogsReloadNanoseconds"))
        XCTAssertTrue(stateSource.contains("sessionLogCache"))
        XCTAssertTrue(stateSource.contains("appendPodLogRead"))
        XCTAssertTrue(stateSource.contains("appendUnifiedServiceLogRead"))
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
        XCTAssertTrue(rootViewSource.contains("let disallowedModifiers: NSEvent.ModifierFlags = [.control]"))
    }

    func testPreferencesExposeArrowKeysForHistoryBindings() throws {
        let testFile = URL(fileURLWithPath: #filePath)
        let root = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let preferencesPath = root.appendingPathComponent("Sources/RuneUI/Views/RunePreferencesView.swift").path
        let preferencesSource = try String(contentsOfFile: preferencesPath, encoding: .utf8)

        XCTAssertTrue(preferencesSource.contains("[\"[\", \"]\", \"left\", \"right\"]"))
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

    private var runeRootViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift").path
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
