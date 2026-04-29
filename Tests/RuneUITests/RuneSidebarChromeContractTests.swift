import XCTest

final class RuneSidebarChromeContractTests: XCTestCase {
    func testSidebarUsesGlassPaneSurfaceInsteadOfRoundedMaterialCard() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        guard let sidebarStart = source.range(of: "private var sidebar: some View {"),
              let sectionRowStart = source.range(of: "private func sectionRow", range: sidebarStart.upperBound..<source.endIndex) else {
            XCTFail("Could not locate sidebar block in RuneRootView.swift")
            return
        }

        let sidebarBlock = String(source[sidebarStart.lowerBound..<sectionRowStart.lowerBound])
        XCTAssertTrue(sidebarBlock.contains("sidebarBrandHeader"))
        XCTAssertTrue(source.contains("Image(\"rune_logo_main\", bundle: .module)"))
        XCTAssertTrue(source.contains(".frame(width: 104, height: 104)"))
        XCTAssertTrue(sidebarBlock.contains("RuneGlassPaneSurface(role: .sidebar)"))
        XCTAssertTrue(sidebarBlock.contains("RuneGlassPaneBorder(role: .sidebar)"))
        XCTAssertFalse(sidebarBlock.contains("RoundedRectangle(cornerRadius:"))
        XCTAssertFalse(sidebarBlock.contains(".thinMaterial"))
        XCTAssertFalse(sidebarBlock.contains(".clipShape("))
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
        XCTAssertTrue(toolbarBlock.contains(".keyboardShortcut(.leftArrow, modifiers: [.control, .option])"))
        XCTAssertTrue(toolbarBlock.contains(".keyboardShortcut(.rightArrow, modifiers: [.control, .option])"))
        XCTAssertFalse(toolbarBlock.contains("Button(\"Palette\")"))
        XCTAssertFalse(toolbarBlock.contains("Button(\"Reload\")"))
    }

    func testHistoryKeyboardMonitorAcceptsControlOptionArrowKeys() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(rootViewSource.contains("historyArrowNavigationAction(for event: NSEvent)"))
        XCTAssertTrue(rootViewSource.contains("guard relevantModifiers == [.control, .option] else { return nil }"))
        XCTAssertTrue(rootViewSource.contains("case 123:"))
        XCTAssertTrue(rootViewSource.contains("return .historyBack"))
        XCTAssertTrue(rootViewSource.contains("case 124:"))
        XCTAssertTrue(rootViewSource.contains("return .historyForward"))
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

    private var runeAppViewModelPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/ViewModels/RuneAppViewModel.swift").path
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
