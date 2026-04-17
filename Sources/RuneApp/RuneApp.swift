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

        DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
            NSApp.activate(ignoringOtherApps: true)

            if let window = NSApp.windows.first {
                window.makeKeyAndOrderFront(nil)
            }

            if ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT"] == "1" {
                NSLog(
                    "[Rune][App] activated reason=%@ windows=%ld keyWindow=%@",
                    reason,
                    NSApp.windows.count,
                    NSApp.keyWindow?.title ?? "nil"
                )
            }
        }
    }
}

@main
struct RuneApplication: App {
    @NSApplicationDelegateAdaptor(RuneAppDelegate.self) private var appDelegate
    @StateObject private var viewModel = RuneAppViewModel()

    var body: some Scene {
        WindowGroup("Rune") {
            RuneRootView(viewModel: viewModel)
                .frame(minWidth: 1280, minHeight: 820)
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
                    viewModel.refreshCurrentView()
                }
                .keyboardShortcut("r", modifiers: .command)

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
        }
    }
}
