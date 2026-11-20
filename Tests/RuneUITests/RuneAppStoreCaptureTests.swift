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

/// App Store screenshots of the app against the local Docker Compose K3s
/// clusters, captured from Rune's own windows at 2880 by 1800 (a 1440 by 900
/// point window on a Retina display). Needs no screen recording permission.
///
/// Runs only when `RUNE_UI_APPSTORE_SCREENSHOT_DIR` is set, the stack is up
/// and `RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1`. `RUNE_UI_APPSTORE_THEME`
/// picks the theme by raw value (default `emberGlass`). Run it on its own:
/// the scripted scenario is switched on for the whole test process.
@MainActor
final class RuneAppStoreCaptureTests: XCTestCase {
    private static let windowSize = NSSize(width: 1440, height: 900)
    private static let steps: [(RuneRootLiveDebugScenarioStep, String)] = [
        (.overview, "01-overview"),
        (.workloadPodLogs, "02-pod-logs"),
        (.workloadDeploymentUnifiedLogs, "03-deployment-logs"),
        (.terminalLogs, "04-terminal-with-logs"),
        (.workloadDeploymentYAMLValidation, "06-yaml-validation"),
        (.rbacCanI, "07-rbac-can-i"),
        (.authDoctor, "08-auth-doctor"),
        (.networkingServiceOverview, "09-networking")
    ]

    func testCaptureAppStoreScenes() async throws {
        let environment = ProcessInfo.processInfo.environment
        guard let directory = environment["RUNE_UI_APPSTORE_SCREENSHOT_DIR"], !directory.isEmpty else {
            throw XCTSkip("Set RUNE_UI_APPSTORE_SCREENSHOT_DIR to capture App Store screenshots.")
        }
        guard environment["RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS"] == "1" else {
            throw XCTSkip("Set RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 with the Docker Compose stack running.")
        }
        let kubeconfig = repoRoot.appendingPathComponent("docker-compose/generated/rune-fake-kubeconfig.yaml")
        let contents = try String(contentsOf: kubeconfig, encoding: .utf8)
        guard contents.contains("server: https://127.0.0.1:16443"),
              contents.contains("server: https://127.0.0.1:17443") else {
            throw XCTSkip("The Docker Compose kubeconfig is not the local-only fake-cluster fixture.")
        }
        let output = URL(fileURLWithPath: directory, isDirectory: true)
        try FileManager.default.createDirectory(at: output, withIntermediateDirectories: true)

        // The scenario reads these once, on first use, so they are set before
        // any RuneRootView exists in this process.
        let stepList = Self.steps.map(\.0.rawValue).joined(separator: ",")
        setenv("RUNE_DEBUG_LAYOUT_LIVE_SCENARIO", "1", 1)
        setenv("RUNE_DEBUG_LAYOUT_LIVE_SCENARIO_STEPS", stepList, 1)
        setenv("RUNE_DEBUG_LAYOUT_CONTEXT", "fake-orbit-mesh", 1)
        setenv("RUNE_DEBUG_LAYOUT_NAMESPACE", "alpha-zone", 1)
        // The multi-container pod, whose logs interleave.
        setenv("RUNE_DEBUG_LAYOUT_POD", "alpha-log-matrix", 1)
        setenv("RUNE_DEBUG_LAYOUT_DETAIL_WIDTH", "640", 1)
        setenv("RUNE_DEBUG_LAYOUT_POD_DWELL_MS", "2500", 1)
        guard RuneRootLayoutDebug.liveScenarioEnabled,
              RuneRootLayoutDebug.liveScenarioSteps.count == Self.steps.count else {
            XCTFail("The scripted scenario was read before this test set it up; run this test on its own.")
            return
        }

        let theme = environment["RUNE_UI_APPSTORE_THEME"].flatMap(RuneAppearanceTheme.init(rawValue:)) ?? .emberGlass
        let volatile = VolatileTestDefaults(self)
        volatile.set(theme.rawValue, forKey: RuneSettingsKeys.appearanceTheme)
        volatile.set(false, forKey: RuneSettingsKeys.enableDemoCluster)
        volatile.set(true, forKey: RuneSettingsKeys.logsShowTimestamps)
        volatile.set(false, forKey: RuneSettingsKeys.saveLastAppState)
        volatile.set(260.0, forKey: RuneSettingsKeys.layoutSidebarWidth)

        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeconfig)])
        let preferencesSuite = TestDefaultsSuite.name("RuneAppStoreCaptureTests.\(UUID().uuidString)")
        let preferencesDefaults = try XCTUnwrap(UserDefaults(suiteName: preferencesSuite))
        defer { preferencesDefaults.removeTestSuite(named: preferencesSuite) }
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: KubernetesClient(commandTimeout: 10),
            bookmarkManager: .inMemory(),
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: preferencesDefaults),
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        try await viewModel.reloadContexts()
        try await waitUntil(timeout: 30) { state.contexts.contains { $0.name == "fake-orbit-mesh" } }
        viewModel.setContext(KubeContext(name: "fake-orbit-mesh"))
        try await waitUntil(timeout: 30) {
            state.selectedNamespace == "alpha-zone" && !state.pods.isEmpty && !state.isLoading
        }
        viewModel.isSidebarVisible = true
        viewModel.isDetailPaneVisible = true

        var captured: [String] = []
        RuneRootLayoutDebug.liveScenarioSnapshotHandler = { [weak self] step in
            guard let self, let window = self.window,
                  let name = Self.steps.first(where: { $0.0 == step })?.1 else { return }
            await self.keepWindowSize(window)
            // The pod list can put the first pod back when it finishes
            // loading; the pod logs picture is of the multi-container pod.
            if step == .workloadPodLogs, state.selectedPod?.name != "alpha-log-matrix",
               let pod = state.pods.first(where: { $0.name == "alpha-log-matrix" }) {
                viewModel.selectPod(pod)
                viewModel.reloadLogsForSelection()
                try? await self.waitUntil(timeout: 15) {
                    state.selectedPod?.name == "alpha-log-matrix" && !state.isLoadingLogs
                        && state.podLogs.contains("alpha-log-matrix")
                }
            }
            try? await Task.sleep(nanoseconds: 1_200_000_000)
            if let image = self.captureWindow(window) {
                try? self.png(image).write(to: output.appendingPathComponent("\(name).png"))
                captured.append(name)
            }

        }
        defer { RuneRootLayoutDebug.liveScenarioSnapshotHandler = nil }

        let window = makeWindow(RuneRootView(
            viewModel: viewModel,
            onLayoutSnapshotChange: nil,
            debugDisableBootstrap: true,
            debugDisableLayoutPersistence: true
        ))
        self.window = window
        defer {
            self.window = nil
            RuneTestWindow.close(window)
        }

        try await waitUntil(timeout: 300) { captured.count == Self.steps.count }
        XCTAssertEqual(Set(captured), Set(Self.steps.map(\.1)), "scenario steps captured")

        // The command palette is a sheet: capture the window's area so the
        // sheet above it is in the picture.
        viewModel.setSection(.workloads)
        viewModel.setWorkloadKind(.pod)
        viewModel.presentCommandPalette(prefilledQuery: ":po ")
        await keepWindowSize(window)
        try await Task.sleep(nanoseconds: 2_000_000_000)
        let palette = try XCTUnwrap(captureArea(of: window))
        try png(palette).write(to: output.appendingPathComponent("05-command-palette.png"))
        viewModel.dismissCommandPalette()

        // Settings in its own window, centred above the app.
        viewModel.setSection(.overview)
        await keepWindowSize(window)
        try await Task.sleep(nanoseconds: 1_500_000_000)
        let settings = NSWindow(
            contentRect: NSRect(origin: .zero, size: NSSize(width: 860, height: 640)),
            styleMask: [.titled, .closable, .fullSizeContentView],
            backing: .buffered,
            defer: false
        )
        let settingsHost = NSHostingController(rootView: RunePreferencesView())
        settingsHost.sizingOptions = []
        settings.contentViewController = settingsHost
        settings.setContentSize(NSSize(width: 860, height: 640))
        settings.setFrameOrigin(NSPoint(
            x: window.frame.midX - settings.frame.width / 2,
            y: window.frame.midY - settings.frame.height / 2 - 30
        ))
        settings.makeKeyAndOrderFront(nil)
        defer { RuneTestWindow.close(settings) }
        try await Task.sleep(nanoseconds: 2_000_000_000)
        let settingsShot = try XCTUnwrap(captureArea(of: window))
        try png(settingsShot).write(to: output.appendingPathComponent("10-settings-over-app.png"))
    }

    private var window: NSWindow?

    private func makeWindow<V: View>(_ view: V) -> NSWindow {
        let window = NSWindow(
            contentRect: NSRect(origin: .zero, size: Self.windowSize),
            styleMask: [.titled, .closable, .resizable, .fullSizeContentView],
            backing: .buffered,
            defer: false
        )
        window.titlebarAppearsTransparent = true
        window.titleVisibility = .hidden
        let host = NSHostingController(rootView: view)
        // Keep the window at exactly 1440 by 900 instead of fitting the content.
        host.sizingOptions = []
        window.contentViewController = host
        window.setFrame(NSRect(origin: NSPoint(x: 40, y: 40), size: Self.windowSize), display: true)
        window.makeKeyAndOrderFront(nil)
        return window
    }

    /// The window grows to fit its content after it appears; put it back to
    /// 1440 by 900 so every picture is 2880 by 1800.
    private func keepWindowSize(_ window: NSWindow) async {
        guard window.frame.size != Self.windowSize else { return }
        window.setFrame(NSRect(origin: window.frame.origin, size: Self.windowSize), display: true)
        try? await Task.sleep(nanoseconds: 600_000_000)
    }

    /// The window alone, with its rounded corners clear.
    private func captureWindow(_ window: NSWindow) -> CGImage? {
        CGWindowListCreateImage(
            .null,
            .optionIncludingWindow,
            CGWindowID(window.windowNumber),
            [.boundsIgnoreFraming, .bestResolution]
        )
    }

    /// Everything of Rune's on screen inside the window's frame: the window
    /// and any sheet or window above it.
    private func captureArea(of window: NSWindow) -> CGImage? {
        guard let screenHeight = NSScreen.screens.first?.frame.height else { return nil }
        let frame = window.frame
        let rect = CGRect(x: frame.minX, y: screenHeight - frame.maxY, width: frame.width, height: frame.height)
        return CGWindowListCreateImage(rect, .optionOnScreenOnly, kCGNullWindowID, [.bestResolution])
    }

    private func png(_ image: CGImage) throws -> Data {
        try XCTUnwrap(NSBitmapImageRep(cgImage: image).representation(using: .png, properties: [:]))
    }

    private func waitUntil(timeout: TimeInterval, _ condition: @escaping @MainActor () -> Bool) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while !condition() {
            guard Date() < deadline else {
                XCTFail("Timed out after \(Int(timeout)) s")
                throw CancellationError()
            }
            try await Task.sleep(nanoseconds: 200_000_000)
        }
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}
#endif
