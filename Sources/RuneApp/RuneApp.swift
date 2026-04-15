import RuneCore
import RuneUI
import SwiftUI

@main
struct RuneApplication: App {
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
