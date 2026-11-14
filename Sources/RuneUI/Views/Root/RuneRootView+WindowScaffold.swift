import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    var configuredMainSplitContainer: some View {
        mainSplitContainer
            .toolbar {
                ToolbarItemGroup(placement: .navigation) {
                    Button {
                        viewModel.navigateBack()
                    } label: {
                        Image(systemName: "chevron.left")
                    }
                    .help("Back (Command-Option-Left Arrow)")
                    .keyboardShortcut(.leftArrow, modifiers: [.command, .option])
                    .disabled(!viewModel.canNavigateBack)

                    Button {
                        viewModel.navigateForward()
                    } label: {
                        Image(systemName: "chevron.right")
                    }
                    .help("Forward (Command-Option-Right Arrow)")
                    .keyboardShortcut(.rightArrow, modifiers: [.command, .option])
                    .disabled(!viewModel.canNavigateForward)

                    Button {
                        viewModel.toggleSidebarVisibility()
                    } label: {
                        Image(systemName: "sidebar.left")
                    }
                    .help(viewModel.isSidebarVisible ? "Hide Sidebar" : "Show Sidebar")

                    Button {
                        viewModel.toggleDetailPaneVisibility()
                    } label: {
                        Image(systemName: "sidebar.right")
                    }
                    .help(viewModel.isDetailPaneVisible ? "Hide Inspector" : "Show Inspector")

                    Menu {
                        ForEach(viewModel.contextMenuOptions) { context in
                            Button(context.name) {
                                viewModel.setContext(context)
                            }
                        }
                        if let context = viewModel.state.selectedContext {
                            Divider()
                            Button {
                                viewModel.toggleProductionMark(for: context)
                            } label: {
                                Label(
                                    viewModel.isManuallyMarkedProduction(context) ? "Unmark Production" : "Mark as Production",
                                    systemImage: viewModel.isManuallyMarkedProduction(context) ? "shield.slash" : "exclamationmark.shield"
                                )
                            }
                        }
                    } label: {
                        Text(viewModel.state.selectedContext?.name ?? "No Context")
                            .runeInterfaceFont(weight: .medium)
                            .lineLimit(1)
                    }
                    .help(viewModel.state.selectedContext?.name ?? "No Context")

                    Menu {
                        ForEach(namespaceSuggestions, id: \.self) { namespace in
                            Button(namespace) {
                                viewModel.setNamespace(namespace)
                            }
                        }
                        if !manualNamespaceMenuOptions.isEmpty {
                            Divider()
                            Section("Manual namespaces") {
                                ForEach(manualNamespaceMenuOptions, id: \.self) { namespace in
                                    Button {
                                        viewModel.setNamespace(namespace)
                                    } label: {
                                        Label(namespace, systemImage: "square.and.pencil")
                                    }
                                }
                            }
                        }
                        Divider()
                        Button {
                            viewModel.toggleFavoriteNamespace(viewModel.state.selectedNamespace)
                        } label: {
                            Label(
                                viewModel.isFavoriteNamespace(viewModel.state.selectedNamespace) ? "Remove Namespace Favorite" : "Favorite Namespace",
                                systemImage: viewModel.isFavoriteNamespace(viewModel.state.selectedNamespace) ? "star.slash" : "star"
                            )
                        }
                        .disabled(viewModel.state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                        Button("Enter Namespace...") {
                            manualNamespaceInput = viewModel.state.selectedNamespace
                            isManualNamespaceSheetPresented = true
                        }
                    } label: {
                        Text(namespaceMenuTitle)
                            .runeInterfaceFont(weight: .medium)
                            .lineLimit(1)
                    }
                    .help(namespaceMenuTitle)
                }

                ToolbarItemGroup(placement: .primaryAction) {
                    Button {
                        viewModel.refreshCurrentView(debounced: false)
                    } label: {
                        Image(systemName: "arrow.clockwise")
                    }
                    .help("Reload")
                    .keyboardShortcut("r", modifiers: .command)

                    Toggle(isOn: $viewModel.isLiveStatusUpdatesEnabled) {
                        Image(systemName: viewModel.isLiveStatusUpdatesEnabled ? "dot.radiowaves.left.and.right" : "pause.circle")
                    }
                    .toggleStyle(.button)
                    .help(viewModel.isLiveStatusUpdatesEnabled ? "Live status updates are on" : "Turn on live status updates")

                    Button {
                        viewModel.presentCommandPalette()
                    } label: {
                        Image(systemName: "command")
                    }
                    .accessibilityIdentifier("rune.command-palette.open")
                    .help("Command Palette")
                    .keyboardShortcut("k", modifiers: .command)

                    Button {
                        openSettingsWindow()
                    } label: {
                        Image(systemName: "gearshape")
                    }
                    .help("Settings")
                    .accessibilityLabel("Settings")
                    .accessibilityIdentifier("rune.settings.open")
                }
            }
            .toolbarBackground(.visible, for: .windowToolbar)
            .sheet(isPresented: commandPalettePresentedBinding) {
                CommandPaletteView(viewModel: viewModel)
            }
            .sheet(isPresented: $isYAMLEditorSheetPresented) {
                yamlManifestEditorSheet()
                    .runePendingWriteConfirmation(
                        isPresented: pendingWriteActionPresentedBinding,
                        title: viewModel.pendingWriteActionTitle,
                        confirmLabel: viewModel.pendingWriteActionConfirmLabel,
                        isDestructive: viewModel.pendingWriteActionIsDestructive,
                        message: viewModel.pendingWriteActionMessage,
                        targetSummary: viewModel.pendingWriteActionTargetSummary,
                        commandPreview: viewModel.pendingWriteActionKubectlCommand,
                        showsCopyCommandAction: !viewModel.pendingWriteActionKubectlCommand.isEmpty,
                        onConfirm: confirmPendingWriteActionFromDialog,
                        onCancel: cancelPendingWriteActionFromDialog,
                        onCopyCommand: viewModel.copyPendingWriteActionKubectlCommand
                    )
            }
            .sheet(item: $selectedAddClusterProvider, onDismiss: {
                addClusterNativeContextRefreshGate.advanceScope()
                isCheckingAddClusterNativeProfiles = false
                viewModel.cancelNativeCloudClusterImport()
            }) { provider in
                addClusterProviderSheet(provider)
            }
            .sheet(isPresented: $isManualNamespaceSheetPresented) {
                manualNamespaceSheet
            }
            .runePendingWriteConfirmation(
                isPresented: rootPendingWriteActionPresentedBinding,
                title: viewModel.pendingWriteActionTitle,
                confirmLabel: viewModel.pendingWriteActionConfirmLabel,
                isDestructive: viewModel.pendingWriteActionIsDestructive,
                message: viewModel.pendingWriteActionMessage,
                targetSummary: viewModel.pendingWriteActionTargetSummary,
                commandPreview: viewModel.pendingWriteActionKubectlCommand,
                showsCopyCommandAction: !viewModel.pendingWriteActionKubectlCommand.isEmpty,
                onConfirm: confirmPendingWriteActionFromDialog,
                onCancel: cancelPendingWriteActionFromDialog,
                onCopyCommand: viewModel.copyPendingWriteActionKubectlCommand
            )
    }

    var resolvedShellVariant: RuneRootShellVariant {
        RuneRootShellVariant.resolved(override: forcedShellVariant)
    }

    var resolvedManifestInlineEditorImplementation: ManifestInlineEditorImplementation {
        ManifestInlineEditorImplementation.resolved(override: forcedManifestInlineEditorImplementation)
    }

    var manualNamespaceSheet: some View {
        return VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogSectionSpacing) {
            Text("Enter Namespace")
                .runeText(.title)
                .accessibilityAddTraits(.isHeader)
            Text("Use this when RBAC does not allow listing namespaces, or when the namespace is not in the cached menu yet.")
                .runeText(.micro)
                .foregroundStyle(.runeSecondary)
                .fixedSize(horizontal: false, vertical: true)

            TextField("namespace", text: $manualNamespaceInput)
                .textFieldStyle(RuneControlTextFieldStyle())
                .runeTextInputCursor()

            RuneDialogActionBar {
                Button {
                    isManualNamespaceSheetPresented = false
                } label: {
                    RuneDialogButtonLabel("Cancel")
                }
                .buttonStyle(RuneToolbarButtonStyle())
                .keyboardShortcut(.cancelAction)

                Button {
                    viewModel.setNamespace(manualNamespaceInput)
                    isManualNamespaceSheetPresented = false
                } label: {
                    RuneDialogButtonLabel("Use Namespace")
                }
                .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
                .keyboardShortcut(.defaultAction)
                .disabled(manualNamespaceInput.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            }
        }
        .padding(RuneUILayoutMetrics.dialogContentPadding)
        .frame(width: RuneUILayoutMetrics.compactDialogWidth)
        .runePointerCursor()
    }
}
