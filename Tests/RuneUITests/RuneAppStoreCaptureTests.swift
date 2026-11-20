// The scripted scenario it drives exists only in debug builds.
#if DEBUG
import AppKit
@testable import RuneUI
import SwiftUI
import XCTest

/// App Store screenshots of the app against the local Docker Compose K3s
/// clusters, captured from Rune's own windows at 2880 by 1800 (a 1440 by 900
/// point window on a Retina display). Needs no screen recording permission.
///
/// Runs only when `RUNE_UI_APPSTORE_SCREENSHOT_DIR` is set; see
/// `RuneAppStoreCaptureFixture` for the rest. Run it on its own.
@MainActor
final class RuneAppStoreCaptureTests: XCTestCase {
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
        let fixture = try await RuneAppStoreCaptureFixture(
            self,
            outputVariable: "RUNE_UI_APPSTORE_SCREENSHOT_DIR",
            steps: Self.steps.map(\.0),
            windowSize: NSSize(width: 1440, height: 900)
        )
        let viewModel = fixture.viewModel
        let output = fixture.output

        var captured: [String] = []
        RuneRootLayoutDebug.liveScenarioSnapshotHandler = { [weak self] step in
            guard let self, let window = fixture.window,
                  let name = Self.steps.first(where: { $0.0 == step })?.1 else { return }
            await fixture.keepWindowSize()
            if step == .workloadPodLogs {
                await fixture.selectLogMatrixPod()
            }
            try? await Task.sleep(nanoseconds: 1_200_000_000)
            if let image = self.captureWindow(window) {
                try? self.png(image).write(to: output.appendingPathComponent("\(name).png"))
                captured.append(name)
            }
        }
        defer { RuneRootLayoutDebug.liveScenarioSnapshotHandler = nil }

        let window = fixture.openWindow()
        defer { fixture.closeWindow() }

        try await fixture.waitUntil(timeout: 300) { captured.count == Self.steps.count }
        XCTAssertEqual(Set(captured), Set(Self.steps.map(\.1)), "scenario steps captured")

        // The command palette is a sheet: capture the window's area so the
        // sheet above it is in the picture.
        viewModel.setSection(.workloads)
        viewModel.setWorkloadKind(.pod)
        viewModel.presentCommandPalette(prefilledQuery: ":po ")
        await fixture.keepWindowSize()
        try await Task.sleep(nanoseconds: 2_000_000_000)
        let palette = try XCTUnwrap(captureArea(of: window))
        try png(palette).write(to: output.appendingPathComponent("05-command-palette.png"))
        viewModel.dismissCommandPalette()

        // Settings in its own window, centred above the app.
        viewModel.setSection(.overview)
        await fixture.keepWindowSize()
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
}
#endif
