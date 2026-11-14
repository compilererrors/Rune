import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    var sidebar: some View {
        VStack(alignment: .leading, spacing: RuneSpacing.md) {
            TextField("Search contexts", text: Binding(get: {
                viewModel.state.contextSearchQuery
            }, set: { newValue in
                viewModel.setContextSearchQuery(newValue)
            }))
            .textFieldStyle(RuneControlTextFieldStyle())
            .focused($textInputFocus, equals: .contextSearch)

            VStack(alignment: .leading, spacing: RuneSpacing.xxs) {
                RuneGroupLabel("Sections", isEmphasized: keyboardPaneFocus == .sidebarSections)
                    .padding(.horizontal, RuneSpacing.xs)

                VStack(alignment: .leading, spacing: RuneSpacing.hairline) {
                    ForEach(RuneSection.allCases) { section in
                        sectionRow(section)
                    }
                }
            }

            VStack(alignment: .leading, spacing: RuneSpacing.xxs) {
                RuneGroupLabel("Contexts", isEmphasized: keyboardPaneFocus == .sidebarContexts) {
                    addClusterButton
                }
                .padding(.leading, RuneSpacing.xs)

                ScrollView {
                    LazyVStack(alignment: .leading, spacing: RuneSpacing.hairline) {
                        if !hasAvailableKubernetesContexts {
                            RuneContentStateView(
                                .empty(
                                    title: "No contexts yet",
                                    message: "Connect from Overview or use Add Cluster above."
                                ),
                                variant: .inline
                            )
                        } else if viewModel.visibleContexts.isEmpty {
                            RuneContentStateView(
                                .filteredEmpty(
                                    title: "No matching contexts",
                                    message: "Clear the search to show all loaded contexts."
                                ),
                                variant: .inline,
                                action: RuneContentStateAction("Clear", systemImage: "xmark.circle") {
                                    viewModel.setContextSearchQuery("")
                                }
                            )
                        } else {
                            ForEach(viewModel.visibleContexts) { context in
                                contextRow(context)
                            }
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .topLeading)
                }
                .accessibilityIdentifier("rune.sidebar.contexts.scroll")
                .frame(minHeight: 80, maxHeight: .infinity, alignment: .topLeading)
                .layoutPriority(1)
            }
            .layoutPriority(1)

            sidebarFooter
        }
        .padding(RuneUILayoutMetrics.sidebarPadding)
        .frame(minWidth: 0, maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background {
            ZStack(alignment: .trailing) {
                RuneGlassPaneSurface(role: .sidebar)
                RuneGlassPaneBorder(role: .sidebar)
                paneFocusOutline(isFocused: keyboardPaneFocus == .sidebarSections || keyboardPaneFocus == .sidebarContexts)
            }
        }
    }

    var sidebarFooter: some View {
        VStack(alignment: .leading, spacing: RuneSpacing.xs) {
            Divider()
            Toggle(isOn: Binding(get: {
                viewModel.state.isReadOnlyMode
            }, set: { value in
                viewModel.setReadOnlyMode(value)
            })) {
                Label("Read-only mode", systemImage: viewModel.state.isReadOnlyMode ? "lock.fill" : "lock.open")
                    .runeText(.bodyEmphasis)
            }
            .toggleStyle(.switch)
            .controlSize(.small)
            .padding(.horizontal, RuneSpacing.xs)
        }
    }

    func sectionRow(_ section: RuneSection) -> some View {
        let isSelected = viewModel.state.selectedSection == section
        return Button {
            viewModel.setSection(section)
        } label: {
            HStack(spacing: RuneSpacing.xs) {
                Image(systemName: section.symbolName)
                    .runeText(.body, weight: isSelected ? .semibold : .regular)
                    .foregroundStyle(isSelected ? AnyShapeStyle(RuneTextStyle.accent) : AnyShapeStyle(.runeSecondary))
                    .frame(width: 18)
                Text(section.localizedTitle(appString))
                    .runeInterfaceFont(weight: isSelected ? .semibold : .medium)
                    .lineLimit(1)
                Spacer(minLength: RuneSpacing.xs)
                Text("⌘" + String(section.commandShortcut))
                    .runeInterfaceFont(relativeSize: -2, design: .monospaced)
                    .foregroundStyle(.runeTertiary)
                    .accessibilityLabel("Command " + String(section.commandShortcut))
            }
            .padding(.horizontal, RuneSpacing.xs)
            .frame(minHeight: RuneControlHeight.regular)
            .contentShape(Rectangle())
            .runeSidebarSelection(isSelected: isSelected)
        }
        .buttonStyle(.plain)
    }

    var addClusterButton: some View {
        Button {
            addClusterPopoverPresented.toggle()
        } label: {
            Label("Add Cluster", systemImage: "plus")
                .labelStyle(.titleAndIcon)
                .padding(.horizontal, RuneSpacing.xs)
                .textCase(nil)
        }
        .accessibilityIdentifier("rune.add-cluster.button")
        .buttonStyle(RuneCompactButtonStyle(role: .secondary))
        .help("Add cluster or kubeconfig")
        .popover(isPresented: $addClusterPopoverPresented, arrowEdge: .trailing) {
            addClusterPopover
        }
        .onChange(of: addClusterPopoverPresented) { _, isPresented in
            guard !isPresented else { return }
            isManualAddClusterExpanded = false
            viewModel.clearManualKubeConfigSecret()
        }
    }

    var addClusterPopover: some View {
        AddClusterPopoverView(
            kubeConfigSourceCount: viewModel.state.kubeConfigSources.count,
            contextCount: viewModel.state.contexts.filter { $0.name != "rune-demo" }.count,
            isLoading: viewModel.state.isLoading || viewModel.isLaunchExperienceVisible,
            externalCommandsAllowed: RuneExternalCommandPolicy.allowsExternalCommands,
            favoriteImportedContexts: $viewModel.favoriteImportedKubeConfigContexts,
            isManualTokenExpanded: $isManualAddClusterExpanded,
            manualContextName: $viewModel.manualKubeConfigName,
            manualServerURL: $viewModel.manualKubeConfigServer,
            manualNamespace: $viewModel.manualKubeConfigNamespace,
            manualBearerToken: $viewModel.manualKubeConfigToken,
            onRefresh: {
                viewModel.refreshKubeConfigSourcesFromDiscovery()
            },
            onImportFile: {
                addClusterPopoverPresented = false
                viewModel.importKubeConfig()
            },
            onPasteKubeconfig: {
                addClusterPopoverPresented = false
                viewModel.importKubeConfigFromPasteboard()
            },
            onImportFolder: {
                addClusterPopoverPresented = false
                viewModel.importKubeConfigFolder()
            },
            onUseDefaultKubeconfig: {
                addClusterPopoverPresented = false
                viewModel.addDefaultKubeConfig()
            },
            onSelectProvider: { provider in
                openAddClusterProviderSheet(provider)
            },
            onImportManualToken: {
                addClusterPopoverPresented = false
                viewModel.importManualTokenKubeConfig()
            }
        )
    }
    func contextRow(_ context: KubeContext) -> some View {
        let displayName = viewModel.contextDisplayName(for: context)
        let secondaryText = viewModel.contextSecondaryDisplayText(for: context)
        let iconName = viewModel.contextDisplayIconName(for: context)
        return ContextSidebarRow(
            displayName: displayName,
            rawName: context.name,
            secondaryText: secondaryText,
            iconName: iconName,
            isSelected: viewModel.state.selectedContext == context,
            isFavorite: viewModel.state.isFavorite(context),
            isProduction: viewModel.isProductionContext(context),
            isManuallyMarkedProduction: viewModel.isManuallyMarkedProduction(context),
            canRemove: context.name != "rune-demo",
            onSelect: {
                viewModel.setContext(context)
            },
            onToggleProduction: {
                viewModel.toggleProductionMark(for: context)
            },
            onToggleFavorite: {
                viewModel.toggleFavorite(for: context)
            },
            onRemove: {
                viewModel.requestKubeConfigContextRemoval(context)
            }
        )
    }
}
