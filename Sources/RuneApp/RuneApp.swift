import AppKit
import OSLog
import RuneCore
import RuneUI
import SwiftUI

private enum RuneWindowLifecycle {
    static let openMainWindowCommandTitle = "New Rune Window"
    static let initialWindowGraceAttempts = 40
    static let maximumOpenAttempts = 60
}

private struct RuneMainWindowCommands: Commands {
    @Environment(\.openWindow) private var openWindow

    var body: some Commands {
        CommandGroup(replacing: .newItem) {
            Button(RuneWindowLifecycle.openMainWindowCommandTitle) {
                openWindow(id: RuneApplicationIdentifiers.mainWindowScene)
            }
            .keyboardShortcut("n", modifiers: .command)
        }
    }
}

@MainActor
private final class RuneAppDelegate: NSObject, NSApplicationDelegate {
    private var didScheduleActivation = false
    private var didFinishRestoringWindows = false
    private var windowRecoveryTask: Task<Void, Never>?
    private var windowRecoveryGeneration: UInt = 0

    override init() {
        super.init()
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(applicationDidFinishRestoringWindows(_:)),
            name: NSApplication.didFinishRestoringWindowsNotification,
            object: nil
        )
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApp.setActivationPolicy(.regular)
        scheduleForegroundActivation(reason: "didFinishLaunching")
        if didFinishRestoringWindows {
            scheduleMainWindowRecovery(allowsInitialWindowGrace: true)
        } else {
            // A LaunchServices `-F` start can finish without replaying the
            // restoration notification to a newly constructed SwiftUI delegate.
            // Give AppKit's normal restoration path first chance, then open the
            // main scene through SwiftUI's native OpenWindowAction.
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) { [weak self] in
                guard let self, !didFinishRestoringWindows else { return }
                didFinishRestoringWindows = true
                RuneLoggers.app.debug("window restoration completion inferred after launch")
                scheduleMainWindowRecovery(allowsInitialWindowGrace: true)
            }
        }
    }

    func applicationDidBecomeActive(_ notification: Notification) {
        scheduleForegroundActivation(reason: "didBecomeActive")
        if didFinishRestoringWindows, let window = mainWindowToPresent() {
            presentOnActiveSpace(window)
        }
    }

    func applicationShouldHandleReopen(
        _ sender: NSApplication,
        hasVisibleWindows flag: Bool
    ) -> Bool {
        guard !flag else { return true }
        scheduleMainWindowRecovery(allowsInitialWindowGrace: false)
        return false
    }

    @objc
    private func applicationDidFinishRestoringWindows(_ notification: Notification) {
        didFinishRestoringWindows = true
        scheduleForegroundActivation(reason: "didFinishRestoringWindows")
        scheduleMainWindowRecovery(allowsInitialWindowGrace: true)
    }

    private func scheduleForegroundActivation(reason: String) {
        guard !didScheduleActivation else { return }
        didScheduleActivation = true

        DispatchQueue.main.async { [weak self] in
            guard let self else { return }
            if let window = self.mainWindowToPresent() {
                self.presentOnActiveSpace(window)
            }

            RuneLoggers.app.debug(
                "activated reason=\(reason, privacy: .public) windows=\(NSApp.windows.count, privacy: .public) keyWindow=\(NSApp.keyWindow?.title ?? "nil", privacy: .private)"
            )
        }
    }

    private func scheduleMainWindowRecovery(allowsInitialWindowGrace: Bool) {
        if let window = mainWindowToPresent() {
            presentOnActiveSpace(window)
            return
        }
        if windowRecoveryTask != nil {
            guard !allowsInitialWindowGrace else { return }
            windowRecoveryTask?.cancel()
            windowRecoveryTask = nil
        }

        windowRecoveryGeneration &+= 1
        let generation = windowRecoveryGeneration
        windowRecoveryTask = Task { @MainActor [weak self] in
            guard let self else { return }
            defer {
                if windowRecoveryGeneration == generation {
                    windowRecoveryTask = nil
                }
            }

            try? await Task.sleep(for: .milliseconds(100))
            guard didFinishRestoringWindows, !Task.isCancelled else { return }

            // WindowGroup creates its first window asynchronously. Give that
            // native path time to materialize before requesting a recovery
            // window, otherwise cold launch can create two main windows.
            if allowsInitialWindowGrace {
                for _ in 0..<RuneWindowLifecycle.initialWindowGraceAttempts {
                    guard !Task.isCancelled else { return }
                    if let window = mainWindowToPresent() {
                        presentOnActiveSpace(window)
                        return
                    }
                    try? await Task.sleep(for: .milliseconds(50))
                }
            }

            var didRequestWindow = false
            for _ in 0..<RuneWindowLifecycle.maximumOpenAttempts {
                guard !Task.isCancelled else { return }
                if let window = mainWindowToPresent() {
                    presentOnActiveSpace(window)
                    return
                }
                if !didRequestWindow,
                   let item = menuItem(
                       titled: RuneWindowLifecycle.openMainWindowCommandTitle,
                       in: NSApp.mainMenu
                   ), let action = item.action,
                   NSApp.sendAction(action, to: item.target, from: item) {
                    didRequestWindow = true
                }
                try? await Task.sleep(for: .milliseconds(50))
            }
            RuneLoggers.app.error("main window could not be opened after launch")
        }
    }

    private func mainWindowToPresent() -> NSWindow? {
        let candidates = NSApp.windows.filter { !($0 is NSPanel) }
        return candidates.first { $0.title == "Rune" } ?? candidates.first
    }

    private func presentOnActiveSpace(_ window: NSWindow) {
        if window.isMiniaturized {
            window.deminiaturize(nil)
        }
        if !window.isOnActiveSpace {
            window.orderOut(nil)
        }
        window.collectionBehavior.insert(.moveToActiveSpace)
        NSApp.activate()
        window.makeKeyAndOrderFront(nil)
    }

    private func menuItem(titled title: String, in menu: NSMenu?) -> NSMenuItem? {
        guard let menu else { return nil }
        for item in menu.items {
            if item.title == title { return item }
            if let match = menuItem(titled: title, in: item.submenu) { return match }
        }
        return nil
    }
}

@main
struct RuneApplication: App {
    @NSApplicationDelegateAdaptor(RuneAppDelegate.self) private var appDelegate
    @StateObject private var viewModel: RuneAppViewModel

    init() {
        RuneSettingsKeys.registerDefaults()
        RuneLaunchEnvironment.applyProcessOverrides()
        _viewModel = StateObject(
            wrappedValue: RuneAppViewModel(
                contextPreferences: FileBackedContextPreferencesStore.applicationSupportStore(),
                lastAppStateStore: JSONLastAppStateStore(),
                terminalWorkspaceStateStore: JSONTerminalWorkspaceStateStore()
            )
        )
    }

    var body: some Scene {
        WindowGroup("Rune", id: RuneApplicationIdentifiers.mainWindowScene) {
            RuneRootView(viewModel: viewModel)
                .frame(
                    minWidth: RuneWindowLayoutDefaults.minimumWidth,
                    minHeight: RuneWindowLayoutDefaults.minimumHeight
                )
                .userActivity(RuneApplicationIdentifiers.openActivityType) { activity in
                    activity.title = "Rune"
                    activity.isEligibleForSearch = true
                }
        }
        Settings {
            RunePreferencesView()
        }
        .commands {
            RuneMainWindowCommands()

            CommandMenu("Rune") {
                Button("Import Kubeconfig...") {
                    viewModel.importKubeConfig()
                }

                Button("Paste Kubeconfig") {
                    viewModel.importKubeConfigFromPasteboard()
                }

                Divider()

                Button("Command Palette") {
                    viewModel.presentCommandPalette()
                }
                .keyboardShortcut("k", modifiers: .command)

                Button("Reload") {
                    viewModel.refreshCurrentView(debounced: false)
                }
                .keyboardShortcut("r", modifiers: .command)

                Divider()

                Button("Run Auth Doctor") {
                    viewModel.runAuthDoctor()
                }

                Button("Load Demo Cluster") {
                    viewModel.loadDemoCluster()
                }

                Button("Reset Demo Cluster") {
                    viewModel.resetDemoCluster()
                }

                Divider()

                Button(viewModel.state.isReadOnlyMode ? "Disable Read-only Mode" : "Enable Read-only Mode") {
                    viewModel.setReadOnlyMode(!viewModel.state.isReadOnlyMode)
                }
            }

            CommandMenu("Sections") {
                ForEach(RuneSection.allCases) { section in
                    Button(section.title) {
                        viewModel.setSection(section)
                    }
                    .keyboardShortcut(KeyEquivalent(section.commandShortcut), modifiers: .command)
                }
            }

            CommandMenu("View") {
                Button("Back") {
                    viewModel.navigateBack()
                }
                .keyboardShortcut(.leftArrow, modifiers: [.command, .option])
                .disabled(!viewModel.canNavigateBack)

                Button("Forward") {
                    viewModel.navigateForward()
                }
                .keyboardShortcut(.rightArrow, modifiers: [.command, .option])
                .disabled(!viewModel.canNavigateForward)

                Divider()

                Button(viewModel.isSidebarVisible ? "Hide Sidebar" : "Show Sidebar") {
                    viewModel.toggleSidebarVisibility()
                }
                .keyboardShortcut("b", modifiers: .command)

                Button(viewModel.isDetailPaneVisible ? "Hide Inspector" : "Show Inspector") {
                    viewModel.toggleDetailPaneVisibility()
                }
                .keyboardShortcut("b", modifiers: [.command, .shift])
            }
        }
    }
}
