// The scripted scenario it drives exists only in debug builds.
#if DEBUG
import AppKit
import RuneCore
import RuneKube
import RuneSecurity
import RuneStore
@testable import RuneUI
import SwiftUI
import XCTest

/// What the App Store screenshots and the App Preview share: Rune signed in
/// to the local Docker Compose K3s clusters (synthetic data only), the
/// scripted scenario switched on for the given steps, and a window of
/// Rune's own at a fixed size.
///
/// Skips unless `outputVariable` names a folder, the stack is up and
/// `RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1`. `RUNE_UI_APPSTORE_THEME` picks
/// the theme by raw value (default `emberGlass`). The scenario is switched on
/// for the whole test process, so each capture test runs on its own.
@MainActor
final class RuneAppStoreCaptureFixture {
    static let podName = "alpha-log-matrix"

    let state: RuneAppState
    let viewModel: RuneAppViewModel
    let output: URL
    private(set) var window: NSWindow?
    private let windowSize: NSSize
    private let volatile: VolatileTestDefaults

    init(
        _ testCase: XCTestCase,
        outputVariable: String,
        steps: [RuneRootLiveDebugScenarioStep],
        windowSize: NSSize,
        context: String = "fake-orbit-mesh",
        extraKubeconfigs: [URL] = []
    ) async throws {
        let environment = ProcessInfo.processInfo.environment
        guard let directory = environment[outputVariable], !directory.isEmpty else {
            throw XCTSkip("Set \(outputVariable) to capture App Store material.")
        }
        guard environment["RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS"] == "1" else {
            throw XCTSkip("Set RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 with the Docker Compose stack running.")
        }
        let kubeconfig = Self.repoRoot.appendingPathComponent("docker-compose/generated/rune-fake-kubeconfig.yaml")
        let contents = try String(contentsOf: kubeconfig, encoding: .utf8)
        guard contents.contains("server: https://127.0.0.1:16443"),
              contents.contains("server: https://127.0.0.1:17443") else {
            throw XCTSkip("The Docker Compose kubeconfig is not the local-only fake-cluster fixture.")
        }
        output = URL(fileURLWithPath: directory, isDirectory: true)
        try FileManager.default.createDirectory(at: output, withIntermediateDirectories: true)
        self.windowSize = windowSize

        // The scenario reads these once, on first use, so they are set before
        // any RuneRootView exists in this process.
        setenv("RUNE_DEBUG_LAYOUT_LIVE_SCENARIO", "1", 1)
        setenv("RUNE_DEBUG_LAYOUT_LIVE_SCENARIO_STEPS", steps.map(\.rawValue).joined(separator: ","), 1)
        setenv("RUNE_DEBUG_LAYOUT_CONTEXT", context, 1)
        setenv("RUNE_DEBUG_LAYOUT_NAMESPACE", "alpha-zone", 1)
        // The multi-container pod, whose logs interleave.
        setenv("RUNE_DEBUG_LAYOUT_POD", Self.podName, 1)
        setenv("RUNE_DEBUG_LAYOUT_DETAIL_WIDTH", "640", 1)
        setenv("RUNE_DEBUG_LAYOUT_POD_DWELL_MS", "2500", 1)
        guard RuneRootLayoutDebug.liveScenarioEnabled,
              RuneRootLayoutDebug.liveScenarioSteps.count == steps.count else {
            throw XCTSkip("The scripted scenario was read before this test set it up; run this test on its own.")
        }

        let theme = environment["RUNE_UI_APPSTORE_THEME"].flatMap(RuneAppearanceTheme.init(rawValue:)) ?? .emberGlass
        volatile = VolatileTestDefaults(testCase)
        volatile.set(theme.rawValue, forKey: RuneSettingsKeys.appearanceTheme)
        volatile.set(false, forKey: RuneSettingsKeys.enableDemoCluster)
        volatile.set(true, forKey: RuneSettingsKeys.logsShowTimestamps)
        volatile.set(false, forKey: RuneSettingsKeys.saveLastAppState)
        volatile.set(260.0, forKey: RuneSettingsKeys.layoutSidebarWidth)
        // Never the export folder of the person running the capture.
        volatile.set(Data(), forKey: RuneSettingsKeys.exportFolderBookmark)
        volatile.set("Rune Exports", forKey: RuneSettingsKeys.exportFolderDisplayName)

        let state = RuneAppState()
        state.setSources(([kubeconfig] + extraKubeconfigs).map { KubeConfigSource(url: $0) })
        let preferencesSuite = TestDefaultsSuite.name("RuneAppStoreCapture.\(UUID().uuidString)")
        let preferencesDefaults = try XCTUnwrap(UserDefaults(suiteName: preferencesSuite))
        testCase.addTeardownBlock { @MainActor in
            preferencesDefaults.removeTestSuite(named: preferencesSuite)
        }
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: KubernetesClient(commandTimeout: 10),
            bookmarkManager: .inMemory(),
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: preferencesDefaults),
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        self.state = state
        self.viewModel = viewModel
        try await viewModel.reloadContexts()
        try await waitUntil(timeout: 30) { state.contexts.contains { $0.name == context } }
        viewModel.setContext(KubeContext(name: context))
        try await waitUntil(timeout: 30) {
            state.selectedNamespace == "alpha-zone" && !state.pods.isEmpty && !state.isLoading
        }
        viewModel.isSidebarVisible = true
        viewModel.isDetailPaneVisible = true
    }

    /// Changes a setting for this test only, as Settings would: the app
    /// hears about it through the usual defaults notification.
    func setSetting(_ value: Any, forKey key: String) {
        volatile.set(value, forKey: key)
        NotificationCenter.default.post(name: UserDefaults.didChangeNotification, object: UserDefaults.standard)
    }

    /// Opens Rune's root view in a window of the fixture's size. Closed by
    /// `closeWindow()`.
    @discardableResult
    func openWindow() -> NSWindow {
        let window = NSWindow(
            contentRect: NSRect(origin: .zero, size: windowSize),
            styleMask: [.titled, .closable, .resizable, .fullSizeContentView],
            backing: .buffered,
            defer: false
        )
        window.titlebarAppearsTransparent = true
        window.titleVisibility = .hidden
        let host = NSHostingController(rootView: RuneRootView(
            viewModel: viewModel,
            onLayoutSnapshotChange: nil,
            debugDisableBootstrap: true,
            debugDisableLayoutPersistence: true
        ))
        // Keep the window at exactly its size instead of fitting the content.
        host.sizingOptions = []
        window.contentViewController = host
        window.setFrame(NSRect(origin: NSPoint(x: 40, y: 40), size: windowSize), display: true)
        window.makeKeyAndOrderFront(nil)
        self.window = window
        return window
    }

    func closeWindow() {
        guard let window else { return }
        self.window = nil
        RuneTestWindow.close(window)
    }

    /// The window grows to fit its content after it appears; put it back so
    /// every picture has the same size.
    func keepWindowSize() async {
        guard let window, window.frame.size != windowSize else { return }
        window.setFrame(NSRect(origin: window.frame.origin, size: windowSize), display: true)
        try? await Task.sleep(nanoseconds: 600_000_000)
    }

    /// The pod list can put the first pod back when it finishes loading; the
    /// pod logs pictures are of the multi-container pod.
    func selectLogMatrixPod() async {
        guard state.selectedPod?.name != Self.podName,
              let pod = state.pods.first(where: { $0.name == Self.podName }) else { return }
        viewModel.selectPod(pod)
        viewModel.reloadLogsForSelection()
        try? await waitUntil(timeout: 15) { [state] in
            state.selectedPod?.name == Self.podName && !state.isLoadingLogs
                && state.podLogs.contains(Self.podName)
        }
    }

    func waitUntil(timeout: TimeInterval, _ condition: @escaping @MainActor () -> Bool) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while !condition() {
            guard Date() < deadline else {
                XCTFail("Timed out after \(Int(timeout)) s")
                throw CancellationError()
            }
            try await Task.sleep(nanoseconds: 200_000_000)
        }
    }

    static var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}
#endif
