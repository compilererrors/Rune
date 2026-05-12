import AppKit
import RuneCore
import RuneUI
import SwiftUI

private final class RuneAppDelegate: NSObject, NSApplicationDelegate {
    private var didScheduleActivation = false

    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApp.setActivationPolicy(.regular)
        scheduleForegroundActivation(reason: "didFinishLaunching")
    }

    func applicationDidBecomeActive(_ notification: Notification) {
        scheduleForegroundActivation(reason: "didBecomeActive")
    }

    private func scheduleForegroundActivation(reason: String) {
        guard !didScheduleActivation else { return }
        didScheduleActivation = true

        DispatchQueue.main.async {
            NSApp.activate(ignoringOtherApps: true)

            if let window = NSApp.windows.first {
                window.makeKeyAndOrderFront(nil)
            }

            NSLog(
                "[Rune][App] activated reason=%@ windows=%ld keyWindow=%@",
                reason,
                NSApp.windows.count,
                NSApp.keyWindow?.title ?? "nil"
            )
        }
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
                contextPreferences: FileBackedContextPreferencesStore.applicationSupportStore()
            )
        )
    }

    var body: some Scene {
        WindowGroup("Rune") {
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
            CommandMenu("Rune") {
                Button("Import Kubeconfig...") {
                    viewModel.importKubeConfig()
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
