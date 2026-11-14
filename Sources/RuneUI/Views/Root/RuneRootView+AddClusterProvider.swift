import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    func addClusterProviderSheet(_ provider: RuneAddClusterProvider) -> some View {
        let selectedNativeContext = selectedAddClusterNativeContextOption
        let isNativeProfileConnected = selectedNativeContext.map {
            connectedAddClusterNativeContextBindingIDs.contains($0.id)
        } ?? false
        let presentation = AddClusterProviderPresentation.resolve(
            provider: provider,
            mode: provider == .local
                ? AddClusterProviderExecutionMode(
                    externalCommandsAllowed: RuneExternalCommandPolicy.allowsExternalCommands
                )
                : .externalCLI,
            isNativeProfileConnected: isNativeProfileConnected
        )
        let optionalCredentialPresentation = AddClusterProviderPresentation.resolve(
            provider: provider,
            mode: .nativeOnly,
            isNativeProfileConnected: isNativeProfileConnected
        )
        let hasCompatibleImportedContext = !addClusterNativeContextOptions.isEmpty
        let primaryAction = presentation.primaryAction(
            hasCompatibleImportedContext: hasCompatibleImportedContext
        )
        let utilityActions = presentation.utilityActions(
            hasCompatibleImportedContext: hasCompatibleImportedContext
        )
        let visibleUtilityActions = utilityActions.filter { action in
            action.id != .runAuthDoctor || !simpleMode
        }
        let advancedImportActions = optionalCredentialPresentation.utilityActions.filter { action in
            action.id == .runNativeImport || action.id == .chooseServiceAccountJSON
        }
        let generalUtilityActions = visibleUtilityActions.filter { action in
            action.id != .runNativeImport && action.id != .chooseServiceAccountJSON
                && (provider.cloudProvider == nil || action.id != .importKubeconfig)
        }
        let canRunCredentialImport = canRunProviderCredentialImport(provider)
        let credentialCommand = providerCredentialCommand(provider, canRunCredentialImport: canRunCredentialImport)
        let runHelp = providerCredentialRunHelp(provider, canRunCredentialImport: canRunCredentialImport)
        let bodyMinimumHeight = presentation.executionMode == .nativeOnly
            && !isAddClusterProviderAdvancedImportExpanded
            && !isAddClusterProviderToolsExpanded
            ? RuneUILayoutMetrics.providerDialogBodyCompactHeight
            : RuneUILayoutMetrics.providerDialogBodyMinHeight
        let bodyIdealHeight: CGFloat = if presentation.executionMode != .nativeOnly {
            RuneUILayoutMetrics.providerDialogBodyIdealHeight
        } else if isAddClusterProviderAdvancedImportExpanded {
            RuneUILayoutMetrics.providerDialogBodyMaxHeight
        } else if isAddClusterProviderToolsExpanded {
            RuneUILayoutMetrics.providerDialogBodyIdealHeight
        } else {
            RuneUILayoutMetrics.providerDialogBodyCompactHeight
        }

        return AddClusterProviderSheetLayout(
            bodyMinimumHeight: bodyMinimumHeight,
            bodyIdealHeight: bodyIdealHeight,
            noticeToken: viewModel.cloudKubeConfigImportDiagnostic.map { "\($0.title)\n\($0.message)" }
                ?? viewModel.cloudKubeConfigImportStatus
        ) {
            HStack(alignment: .top, spacing: RuneSpacing.sm) {
                Button {
                    closeAddClusterProviderSheet(showPopover: true)
                } label: {
                    Image(systemName: "chevron.left")
                        .frame(
                            width: RuneUILayoutMetrics.dialogIconButtonSize,
                            height: RuneUILayoutMetrics.dialogIconButtonSize
                        )
                        .contentShape(Rectangle())
                }
                .buttonStyle(RuneCompactButtonStyle())
                .help("Back to Add Cluster")
                .accessibilityLabel("Back to Add Cluster")
                .disabled(
                    viewModel.isRunningCloudKubeConfigImport
                        || viewModel.isRunningNativeCloudClusterImport
                        || viewModel.isSigningInToAzure
                )

                Image(systemName: presentation.symbolName)
                    .font(.system(size: RuneGlyphSize.dialog, weight: .semibold))
                    .foregroundStyle(provider.accent)
                    .frame(width: 34, height: 34)
                    .background(Circle().fill(provider.accent.opacity(0.14)))
                VStack(alignment: .leading, spacing: RuneSpacing.xxs) {
                    Text(presentation.title)
                        .runeInterfaceFont(relativeSize: 2, weight: .semibold)
                        .accessibilityAddTraits(.isHeader)
                    Text(presentation.subtitle)
                        .runeInterfaceFont()
                        .foregroundStyle(.runeSecondary)
                }
                Spacer(minLength: 0)
            }
        } content: {
                VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogSectionSpacing) {
                    if let diagnostic = viewModel.cloudKubeConfigImportDiagnostic {
                        addClusterCloudImportDiagnosticView(diagnostic, provider: provider)
                            .accessibilityIdentifier("rune.add-cluster.provider.diagnostic")
                    } else if provider.cloudProvider != nil,
                              let status = viewModel.cloudKubeConfigImportStatus {
                        addClusterProviderStatusView(status)
                    }

                    if provider.cloudProvider != nil {
                        addClusterProviderRecommendedPath(
                            provider: provider,
                            presentation: presentation
                        )
                        Button {
                            closeAddClusterProviderSheet()
                            viewModel.importKubeConfig()
                        } label: {
                            Label("Import kubeconfig…", systemImage: "doc.badge.plus")
                        }
                        .buttonStyle(RuneToolbarButtonStyle())
                        .disabled(viewModel.isRunningCloudKubeConfigImport || viewModel.isSigningInToAzure)
                        .help("Choose a kubeconfig file and review its contexts before adding them.")
                        .accessibilityIdentifier("rune.add-cluster.provider.import-kubeconfig")
                    } else {
                        Text(presentation.note)
                            .runeInterfaceFont(relativeSize: -1)
                            .foregroundStyle(.runeSecondary)
                            .fixedSize(horizontal: false, vertical: true)
                    }

                    if presentation.requiresCompatibleImportedContext {
                        AddClusterNativeContextSection(
                            options: addClusterNativeContextOptions,
                            selectedBindingID: $selectedAddClusterNativeContextBindingID,
                            connectedBindingIDs: connectedAddClusterNativeContextBindingIDs,
                            isCheckingProfiles: isCheckingAddClusterNativeProfiles,
                            analysisMessage: addClusterNativeContextAnalysisMessage
                        )
                    }

                    if !presentation.fields.isEmpty
                        && provider != .local
                        && (!presentation.requiresCompatibleImportedContext || selectedNativeContext != nil) {
                        Group {
                            if provider.cloudProvider != nil {
                                addClusterProviderFormSection("Cluster") {
                                    providerCredentialFields(presentation.fields)
                                }

                                addClusterProviderAdvancedImportSection(
                                    presentation: optionalCredentialPresentation,
                                    sharedFields: presentation.fields,
                                    actions: advancedImportActions,
                                    provider: provider,
                                    credentialCommand: credentialCommand,
                                    selectedNativeContext: selectedNativeContext
                                )
                            } else {
                                addClusterProviderFormSection(presentation.credentialSectionTitle) {
                                    providerCredentialFields(presentation.fields)
                                }
                            }
                        }
                        .disabled(viewModel.isRunningCloudKubeConfigImport || viewModel.isSigningInToAzure)
                    }

                    if !generalUtilityActions.isEmpty {
                        Group {
                            if provider.cloudProvider != nil {
                                RuneDisclosureSection(
                                    "Help & tools",
                                    isExpanded: $isAddClusterProviderToolsExpanded,
                                    accessibilityIdentifier: "rune.add-cluster.provider.tools"
                                ) {
                                    addClusterProviderLocalSetupHelp(presentation)
                                        .padding(.vertical, RuneUILayoutMetrics.dialogControlSpacing)
                                    LazyVGrid(columns: addClusterProviderActionColumns, spacing: RuneUILayoutMetrics.dialogControlSpacing) {
                                        ForEach(generalUtilityActions) { action in
                                            addClusterProviderUtilityAction(
                                                action,
                                                provider: provider,
                                                credentialCommand: credentialCommand,
                                                selectedNativeContext: selectedNativeContext
                                            )
                                        }
                                    }
                                    .runeInterfaceControlSize()
                                    .runeInsetCard(padding: 12)
                                    .padding(.top, RuneSpacing.xs)
                                } label: {
                                    Label("Help & tools", systemImage: "wrench.and.screwdriver")
                                        .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                                }
                            } else {
                                addClusterProviderFormSection("Tools") {
                                    LazyVGrid(columns: addClusterProviderActionColumns, spacing: RuneUILayoutMetrics.dialogControlSpacing) {
                                        ForEach(generalUtilityActions) { action in
                                            addClusterProviderUtilityAction(
                                                action,
                                                provider: provider,
                                                credentialCommand: credentialCommand,
                                                selectedNativeContext: selectedNativeContext
                                            )
                                        }
                                    }
                                    .runeInterfaceControlSize()
                                }
                            }
                        }
                        .disabled(viewModel.isRunningCloudKubeConfigImport || viewModel.isSigningInToAzure)
                    }

                    if viewModel.cloudKubeConfigImportDiagnostic == nil,
                       let nativeStatus = viewModel.nativeKubernetesAuthStatus,
                       nativeStatus != viewModel.cloudKubeConfigImportStatus {
                        Text(nativeStatus)
                            .runeInterfaceFont(relativeSize: -1)
                            .foregroundStyle(.runeSecondary)
                            .accessibilityLabel("Authentication status: \(nativeStatus)")
                    }

                    if provider.cloudProvider != nil,
                       !viewModel.cloudKubeConfigImportOutput.isEmpty {
                        addClusterCloudImportOutputView(viewModel.cloudKubeConfigImportOutput)
                    }

                    if presentation.showsCommandDetails {
                        RuneDisclosureSection(
                            "Command Details",
                            isExpanded: $isAddClusterProviderCommandDetailsExpanded,
                            accessibilityIdentifier: "rune.add-cluster.provider.command-details"
                        ) {
                            VStack(alignment: .leading, spacing: RuneSpacing.xs) {
                                Text(credentialCommand)
                                    .runeInterfaceFont(relativeSize: -1, design: .monospaced)
                                    .textSelection(.enabled)
                                    .padding(RuneSpacing.inset)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .background(RuneSurfaceBackground(kind: .inset))

                                ForEach(provider.auxiliaryCommands, id: \.title) { item in
                                    HStack(spacing: RuneSpacing.xs) {
                                        Text(item.title)
                                            .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                                            .frame(width: 54, alignment: .leading)
                                        Text(item.command)
                                            .runeInterfaceFont(relativeSize: -1, design: .monospaced)
                                            .lineLimit(1)
                                            .truncationMode(.middle)
                                            .textSelection(.enabled)
                                        Spacer(minLength: 0)
                                        Button {
                                            copyToPasteboard(item.command)
                                        } label: {
                                            Image(systemName: "doc.on.doc")
                                                .frame(
                                                    width: RuneUILayoutMetrics.dialogIconButtonSize,
                                                    height: RuneUILayoutMetrics.dialogIconButtonSize
                                                )
                                                .contentShape(Rectangle())
                                        }
                                        .buttonStyle(RuneCompactButtonStyle())
                                        .help("Copy \(item.title.lowercased()) command")
                                        .accessibilityLabel("Copy \(item.title) command")
                                    }
                                    .padding(RuneSpacing.xs)
                                    .background(RuneSurfaceBackground(kind: .inset))
                                }
                            }
                            .padding(.top, RuneSpacing.xs)
                        } label: {
                            Label("Command Details", systemImage: "terminal")
                                .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                        }
                    }
                }
        } actions: {
                if viewModel.isSigningInToAzure {
                    Button(role: .cancel) {
                        viewModel.cancelAzureCLISignIn()
                    } label: {
                        RuneDialogButtonLabel("Cancel Sign-in")
                    }
                    .buttonStyle(RuneToolbarButtonStyle())
                    .keyboardShortcut(.cancelAction)
                } else if viewModel.isRunningCloudKubeConfigImport {
                    Button(role: .cancel) {
                        viewModel.cancelCloudKubeConfigImport()
                    } label: {
                        RuneDialogButtonLabel("Cancel Import")
                    }
                    .buttonStyle(RuneToolbarButtonStyle())
                    .keyboardShortcut(.cancelAction)
                } else {
                    Button {
                        closeAddClusterProviderSheet()
                    } label: {
                        RuneDialogButtonLabel("Close")
                    }
                    .buttonStyle(RuneToolbarButtonStyle())
                    .keyboardShortcut(.cancelAction)
                }

                if !viewModel.isSigningInToAzure {
                    providerPrimaryAction(
                        primaryAction,
                        provider: provider,
                        canRunCredentialImport: canRunCredentialImport,
                        runHelp: runHelp
                    )
                }
        }
        .animation(.easeInOut(duration: 0.18), value: isAddClusterProviderAdvancedImportExpanded)
        .animation(.easeInOut(duration: 0.18), value: isAddClusterProviderToolsExpanded)
        .runePointerCursor()
        .interactiveDismissDisabled(
            viewModel.isRunningCloudKubeConfigImport
                || viewModel.isRunningNativeCloudClusterImport
                || viewModel.isSigningInToAzure
        )
        .task(id: "\(provider.rawValue)-\(viewModel.isConnectingNativeKubernetesAuth)") {
            guard presentation.requiresCompatibleImportedContext else { return }
            let scopeGeneration = addClusterNativeContextRefreshGate.scopeGeneration
            await refreshAddClusterNativeContexts(
                for: provider,
                expectedScopeGeneration: scopeGeneration
            )
        }
    }

    func addClusterCloudImportOutputView(_ output: String) -> some View {
        RuneDisclosureSection(
            "Login Output",
            isExpanded: $isAddClusterProviderLoginOutputExpanded,
            accessibilityIdentifier: "rune.add-cluster.provider.login-output"
        ) {
            ScrollView {
                Text(output)
                    .runeInterfaceFont(relativeSize: -2, design: .monospaced)
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(RuneSpacing.inset)
            }
            .frame(maxHeight: 140)
            .background(RuneSurfaceBackground(kind: .inset))
            .padding(.top, RuneSpacing.xs)
        } label: {
            Label("Login Output", systemImage: "terminal")
                .runeInterfaceFont(relativeSize: -1, weight: .semibold)
        }
    }

    func addClusterCloudImportDiagnosticView(
        _ diagnostic: AddClusterCloudImportDiagnostic,
        provider: RuneAddClusterProvider
    ) -> some View {
        VStack(alignment: .leading, spacing: RuneSpacing.inset) {
            HStack(alignment: .top, spacing: RuneSpacing.inset) {
                Image(systemName: "exclamationmark.octagon.fill")
                    .font(.system(size: RuneGlyphSize.sheetHeader, weight: .semibold))
                    .foregroundStyle(.runeDanger)
                VStack(alignment: .leading, spacing: RuneSpacing.xxs) {
                    Text(diagnostic.title)
                        .runeInterfaceFont(weight: .semibold)
                        .fixedSize(horizontal: false, vertical: true)
                    Text(diagnostic.classification)
                        .runeInterfaceFont(relativeSize: -2, weight: .semibold)
                        .foregroundStyle(.runeDanger)
                }
                Spacer(minLength: 0)

                Button {
                    viewModel.clearCloudKubeConfigImportStatus()
                    viewModel.clearNativeKubernetesAuthStatus()
                } label: {
                    Image(systemName: "xmark")
                        .frame(
                            width: RuneUILayoutMetrics.dialogIconButtonSize,
                            height: RuneUILayoutMetrics.dialogIconButtonSize
                        )
                        .contentShape(Rectangle())
                }
                .buttonStyle(RuneCompactButtonStyle())
                .help("Dismiss provider warning")
                .accessibilityLabel("Dismiss provider warning")
            }

            Text(diagnostic.message)
                .runeInterfaceFont(relativeSize: -1)
                .foregroundStyle(.runePrimary)
                .fixedSize(horizontal: false, vertical: true)

            Text(diagnostic.operationShape)
                .runeInterfaceFont(relativeSize: -2, design: .monospaced)
                .foregroundStyle(.runeSecondary)
                .lineLimit(2)
                .textSelection(.enabled)

            HStack(alignment: .top, spacing: RuneSpacing.xs) {
                Label(diagnostic.nextAction, systemImage: "arrow.triangle.2.circlepath")
                    .runeInterfaceFont(relativeSize: -1)
                    .foregroundStyle(.runePrimary)
                    .fixedSize(horizontal: false, vertical: true)

                Spacer(minLength: 0)
            }
            .padding(RuneSpacing.xs)
            .background(RoundedRectangle(cornerRadius: RuneRadius.chip).fill(Color.primary.opacity(0.045)))

            (dynamicTypeSize.isAccessibilitySize
                ? AnyLayout(VStackLayout(alignment: .leading, spacing: RuneUILayoutMetrics.dialogControlSpacing))
                : AnyLayout(HStackLayout(spacing: RuneUILayoutMetrics.dialogControlSpacing))) {
                Link(destination: diagnostic.documentationURL) {
                    Label(diagnostic.documentationTitle, systemImage: "book")
                }
                .runeInterfaceFont(relativeSize: -2, weight: .semibold)

                if !dynamicTypeSize.isAccessibilitySize {
                    Spacer(minLength: 0)
                }

                if diagnostic.recoveryAction == .signInToAzure,
                   provider.cloudProvider == .aks {
                    Button {
                        viewModel.signInToAzureAndRetry(
                            cloudCredentialDraft.request(provider: .aks)
                        )
                    } label: {
                        Label("Sign in to Azure & Retry", systemImage: "person.badge.key")
                    }
                    .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
                    .controlSize(.small)
                    .disabled(viewModel.isSigningInToAzure || viewModel.isRunningCloudKubeConfigImport)
                    .help("Open secure Microsoft sign-in, then retry this AKS import automatically.")
                }
            }
        }
        .padding(RuneSpacing.sm)
        .background(RoundedRectangle(cornerRadius: RuneRadius.card).fill(RuneTextStyle.danger.opacity(0.055)))
        .overlay {
            RoundedRectangle(cornerRadius: RuneRadius.card)
                .stroke(RuneTextStyle.danger.opacity(0.24), lineWidth: 1)
        }
        .accessibilityElement(children: .contain)
    }

    func addClusterProviderStatusView(_ status: String) -> some View {
        HStack(spacing: RuneSpacing.xs) {
            if viewModel.isRunningCloudKubeConfigImport || viewModel.isSigningInToAzure {
                ProgressView()
                    .controlSize(.small)
            } else {
                Image(systemName: "info.circle")
                    .foregroundStyle(.runeSecondary)
            }
            Text(status)
                .runeInterfaceFont(relativeSize: -1, weight: .medium)
                .foregroundStyle(.runeSecondary)
                .fixedSize(horizontal: false, vertical: true)
            Spacer(minLength: 0)
        }
        .accessibilityElement(children: .combine)
        .accessibilityLabel("Provider status: \(status)")
    }

    var addClusterProviderActionColumns: [GridItem] {
        if dynamicTypeSize.isAccessibilitySize {
            return [GridItem(.flexible(), spacing: RuneAddClusterProviderActionLayout.columnSpacing)]
        }
        return [GridItem(
            .adaptive(minimum: RuneAddClusterProviderActionLayout.minimumButtonWidth),
            spacing: RuneAddClusterProviderActionLayout.columnSpacing
        )]
    }

    var selectedAddClusterNativeContextOption: AddClusterNativeContextOption? {
        guard let selectedAddClusterNativeContextBindingID else { return nil }
        return addClusterNativeContextOptions.first { $0.id == selectedAddClusterNativeContextBindingID }
    }

    @ViewBuilder
    func addClusterProviderUtilityAction(
        _ action: AddClusterProviderAction,
        provider: RuneAddClusterProvider,
        credentialCommand: String,
        selectedNativeContext: AddClusterNativeContextOption?
    ) -> some View {
        switch action.id {
        case .importKubeconfig:
            Button {
                closeAddClusterProviderSheet()
                viewModel.importKubeConfig()
            } label: {
                Label(action.title, systemImage: action.systemImage)
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(RuneToolbarButtonStyle())
            .help("Import another kubeconfig and review its contexts before adding them.")

        case .runNativeImport:
            if let cloudProvider = provider.cloudProvider,
               provider == .aks || provider == .eks {
                let canRunOptionalImport = cloudCredentialDraft.hasRequiredNativeFields(for: cloudProvider)
                Button {
                    runNativeCloudImport(provider)
                } label: {
                    Label(action.title, systemImage: action.systemImage)
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(RuneToolbarButtonStyle())
                .disabled(
                    !canRunOptionalImport
                        || viewModel.isRunningCloudKubeConfigImport
                )
                .help(optionalNativeImportHelp(provider, isReady: canRunOptionalImport))
            }

        case .chooseServiceAccountJSON:
            let canRunOptionalImport = cloudCredentialDraft.hasRequiredNativeFields(for: .gke)
            Button {
                chooseAndRunNativeGKEImport()
            } label: {
                Label(action.title, systemImage: action.systemImage)
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(RuneToolbarButtonStyle())
            .disabled(
                provider != .gke
                    || !canRunOptionalImport
                    || viewModel.isRunningCloudKubeConfigImport
            )
            .help(optionalNativeImportHelp(.gke, isReady: canRunOptionalImport))

        case .copyExternalCommand, .copyLocalSetupCommand:
            Button {
                copyToPasteboard(credentialCommand)
            } label: {
                Label(action.title, systemImage: action.systemImage)
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(RuneToolbarButtonStyle())
            .help(action.id == .copyLocalSetupCommand
                ? "Copy the local cluster setup command."
                : "Copy the provider CLI command.")

        case .refreshContexts:
            Button {
                viewModel.refreshKubeConfigSourcesFromDiscovery()
                let scopeGeneration = addClusterNativeContextRefreshGate.scopeGeneration
                Task { @MainActor in
                    try? await Task.sleep(nanoseconds: 250_000_000)
                    await refreshAddClusterNativeContexts(
                        for: provider,
                        expectedScopeGeneration: scopeGeneration
                    )
                }
            } label: {
                Label(action.title, systemImage: action.systemImage)
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(RuneToolbarButtonStyle())
            .help(provider == .local
                ? "Refresh detected contexts after your local cluster tool writes kubeconfig."
                : "Refresh imported contexts and native credential status.")

        case .runAuthDoctor:
            Button {
                viewModel.runAuthDoctor()
            } label: {
                Label(action.title, systemImage: action.systemImage)
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(RuneToolbarButtonStyle())
            .disabled(viewModel.state.isRunningAuthDoctor)
            .help("Run Auth Doctor for provider login, kubeconfig, RBAC, and API access checks.")

        case .disconnectNativeCredentials:
            if let selectedNativeContext, let expectedProvider = provider.nativeAuthProvider {
                Button(role: .destructive) {
                    viewModel.disconnectNativeAuth(
                        request: selectedNativeContext.request,
                        expectedProvider: expectedProvider
                    )
                } label: {
                    Label(action.title, systemImage: action.systemImage)
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(RuneToolbarButtonStyle())
                .disabled(viewModel.isConnectingNativeKubernetesAuth)
                .help("Remove native credentials only for \(selectedNativeContext.contextName).")
            }

        case .runExternalCLI, .connectNativeCredentials:
            EmptyView()
        }
    }

    func optionalNativeImportHelp(
        _ provider: RuneAddClusterProvider,
        isReady: Bool
    ) -> String {
        if isReady {
            switch provider {
            case .aks:
                return "Fetch AKS access using the optional service-principal method."
            case .eks:
                return "Fetch EKS access using the optional access-key method."
            case .gke:
                return "Choose a service-account JSON file and fetch GKE access using the optional service-account method."
            case .local:
                return ""
            }
        }
        guard let cloudProvider = provider.cloudProvider,
              let missingFields = cloudCredentialDraft.missingRequiredNativeFieldSummary(for: cloudProvider) else {
            return "Complete the fields for this optional import method."
        }
        return "Optional method: enter \(missingFields) to continue."
    }

    @MainActor
    func refreshAddClusterNativeContexts(
        for provider: RuneAddClusterProvider,
        expectedScopeGeneration: UInt64
    ) async {
        guard selectedAddClusterProvider == provider,
              let requestToken = addClusterNativeContextRefreshGate.begin(
                expectedScopeGeneration: expectedScopeGeneration
              )
        else {
            return
        }

        guard !RuneExternalCommandPolicy.allowsExternalCommands,
              let nativeProvider = provider.nativeAuthProvider else {
            guard addClusterNativeContextRefreshGate.isCurrent(requestToken),
                  selectedAddClusterProvider == provider else { return }
            addClusterNativeContextOptions = []
            selectedAddClusterNativeContextBindingID = nil
            connectedAddClusterNativeContextBindingIDs = []
            addClusterNativeContextAnalysisMessage = nil
            isCheckingAddClusterNativeProfiles = false
            return
        }

        isCheckingAddClusterNativeProfiles = true
        addClusterNativeContextAnalysisMessage = nil
        defer {
            if addClusterNativeContextRefreshGate.isCurrent(requestToken),
               selectedAddClusterProvider == provider {
                isCheckingAddClusterNativeProfiles = false
            }
        }

        do {
            let sources = viewModel.state.kubeConfigSources
            let analysis = try await Task.detached(priority: .userInitiated) {
                try KubeConfigNativeAuthAnalyzer().analyze(sources: sources)
            }.value
            try Task.checkCancellation()
            guard addClusterNativeContextRefreshGate.isCurrent(requestToken),
                  selectedAddClusterProvider == provider,
                  viewModel.state.kubeConfigSources == sources else { return }

            let options = AddClusterNativeContextResolver.compatibleOptions(
                provider: nativeProvider,
                analysis: analysis
            )
            addClusterNativeContextOptions = options

            if let selectedAddClusterNativeContextBindingID,
               options.contains(where: { $0.id == selectedAddClusterNativeContextBindingID }) {
                // Preserve an explicit picker choice while refreshing profile status.
            } else {
                switch AddClusterNativeContextResolver.resolve(
                    provider: nativeProvider,
                    analysis: analysis,
                    currentContextName: viewModel.state.selectedContext?.name
                ) {
                case .selected(let option):
                    selectedAddClusterNativeContextBindingID = option.id
                case .requiresChoice, .unavailable:
                    selectedAddClusterNativeContextBindingID = nil
                }
            }

            var connected = Set<String>()
            for option in options {
                try Task.checkCancellation()
                guard addClusterNativeContextRefreshGate.isCurrent(requestToken),
                      selectedAddClusterProvider == provider,
                      viewModel.state.kubeConfigSources == sources else { return }
                if let status = try? await viewModel.nativeAuthProfileStatus(for: option.request),
                   status.isConnected {
                    connected.insert(option.id)
                }
            }
            guard addClusterNativeContextRefreshGate.isCurrent(requestToken),
                  selectedAddClusterProvider == provider,
                  viewModel.state.kubeConfigSources == sources else { return }
            connectedAddClusterNativeContextBindingIDs = connected
            if options.isEmpty {
                addClusterNativeContextAnalysisMessage = "Import a \(provider.title) kubeconfig to add this cluster. Rune checks its authentication settings after import."
            }
        } catch is CancellationError {
            return
        } catch {
            guard addClusterNativeContextRefreshGate.isCurrent(requestToken),
                  selectedAddClusterProvider == provider else { return }
            addClusterNativeContextOptions = []
            selectedAddClusterNativeContextBindingID = nil
            connectedAddClusterNativeContextBindingIDs = []
            addClusterNativeContextAnalysisMessage = "Rune could not read the imported contexts. Import a kubeconfig or try Refresh."
        }
    }

    func openAddClusterProviderSheet(_ provider: RuneAddClusterProvider) {
        addClusterNativeContextRefreshGate.advanceScope()
        viewModel.clearManualKubeConfigSecret()
        if !viewModel.isRunningCloudKubeConfigImport {
            viewModel.clearCloudKubeConfigImportStatus()
        }
        viewModel.clearNativeKubernetesAuthStatus()
        addClusterNativeContextOptions = []
        selectedAddClusterNativeContextBindingID = nil
        connectedAddClusterNativeContextBindingIDs = []
        addClusterNativeContextAnalysisMessage = nil
        isCheckingAddClusterNativeProfiles = false
        isAddClusterProviderAdvancedImportExpanded = false
        isAddClusterProviderToolsExpanded = false
        isAddClusterProviderCommandDetailsExpanded = false
        isAddClusterProviderLoginOutputExpanded = false
        addClusterPopoverPresented = false
        selectedAddClusterProvider = provider
    }

    func closeAddClusterProviderSheet(showPopover: Bool = false) {
        addClusterNativeContextRefreshGate.advanceScope()
        selectedAddClusterProvider = nil
        addClusterNativeContextOptions = []
        selectedAddClusterNativeContextBindingID = nil
        connectedAddClusterNativeContextBindingIDs = []
        addClusterNativeContextAnalysisMessage = nil
        isCheckingAddClusterNativeProfiles = false
        isAddClusterProviderAdvancedImportExpanded = false
        isAddClusterProviderToolsExpanded = false
        isAddClusterProviderCommandDetailsExpanded = false
        isAddClusterProviderLoginOutputExpanded = false
        if !showPopover {
            resetAddClusterProviderSheetStateIfIdle()
        }
        guard showPopover else { return }
        DispatchQueue.main.async {
            addClusterPopoverPresented = true
        }
    }

    func resetAddClusterProviderSheetStateIfIdle() {
        guard !viewModel.isRunningCloudKubeConfigImport,
              !viewModel.isSigningInToAzure else { return }
        cloudCredentialDraft = CloudCredentialDraft()
        viewModel.clearCloudKubeConfigImportStatus()
    }

    @ViewBuilder
    func providerCredentialFields(_ fields: [AddClusterProviderField]) -> some View {
        AddClusterProviderCredentialGrid(fields: fields) { field in
            providerCredentialInput(field)
        }
    }

    func addClusterProviderAdvancedImportSection(
        presentation: AddClusterProviderPresentation,
        sharedFields: [AddClusterProviderField],
        actions: [AddClusterProviderAction],
        provider: RuneAddClusterProvider,
        credentialCommand: String,
        selectedNativeContext: AddClusterNativeContextOption?
    ) -> some View {
        RuneDisclosureSection(
            "Optional \(presentation.credentialSectionTitle)",
            isExpanded: $isAddClusterProviderAdvancedImportExpanded,
            accessibilityIdentifier: "rune.add-cluster.provider.advanced-import"
        ) {
            VStack(alignment: .leading, spacing: RuneSpacing.inset) {
                Text(presentation.credentialSectionDescription)
                    .runeInterfaceFont(relativeSize: -1)
                    .foregroundStyle(.runeSecondary)
                    .fixedSize(horizontal: false, vertical: true)

                providerCredentialFields(presentation.credentialFields(excluding: sharedFields))

                if let cloudProvider = provider.cloudProvider,
                   let missingFields = cloudCredentialDraft.missingRequiredNativeFieldSummary(for: cloudProvider) {
                    Label("Required for this method: \(missingFields).", systemImage: "info.circle")
                        .runeInterfaceFont(relativeSize: -1)
                        .foregroundStyle(.runeSecondary)
                        .fixedSize(horizontal: false, vertical: true)
                }

                ForEach(actions) { action in
                    addClusterProviderUtilityAction(
                        action,
                        provider: provider,
                        credentialCommand: credentialCommand,
                        selectedNativeContext: selectedNativeContext
                    )
                    .runeInterfaceControlSize()
                }
            }
            .runeInsetCard(padding: 12)
            .padding(.top, RuneSpacing.xs)
        } label: {
            HStack(spacing: RuneSpacing.xs) {
                Label("Advanced", systemImage: "slider.horizontal.3")
                    .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                Spacer(minLength: 8)
                Text(presentation.credentialSectionTitle)
                    .runeInterfaceFont(relativeSize: -1)
                    .foregroundStyle(.runeSecondary)
                Text("Optional")
                    .runeInterfaceFont(relativeSize: -2, weight: .semibold)
                    .foregroundStyle(.runeSecondary)
                    .padding(.horizontal, RuneSpacing.tight)
                    .padding(.vertical, RuneSpacing.xxs)
                    .background(Capsule().fill(Color.secondary.opacity(0.10)))
            }
        }
    }

    func addClusterProviderRecommendedPath(
        provider: RuneAddClusterProvider,
        presentation: AddClusterProviderPresentation
    ) -> some View {
        let cliName = provider.cloudCLIName ?? "provider CLI"

        return HStack(alignment: .top, spacing: RuneSpacing.inset) {
                Image(systemName: "terminal.fill")
                    .font(.system(size: RuneGlyphSize.tile, weight: .semibold))
                    .foregroundStyle(provider.accent)
                    .frame(width: 26, height: 26)
                    .background(Circle().fill(provider.accent.opacity(0.12)))

                VStack(alignment: .leading, spacing: RuneSpacing.hairline) {
                    Text("Connect with \(cliName)")
                        .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                    Text("Recommended")
                        .runeInterfaceFont(relativeSize: -2, weight: .semibold)
                        .foregroundStyle(.runeAccent)
                    Text(presentation.note)
                        .runeInterfaceFont(relativeSize: -2)
                        .foregroundStyle(.runeSecondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }
        .runeInsetCard(padding: 12)
        .help("Rune runs the provider CLI locally, reads its isolated kubeconfig, and opens review automatically.")
        .accessibilityElement(children: .combine)
        .accessibilityLabel("Connect with \(cliName), recommended")
        .accessibilityHint("Rune runs the provider CLI locally and opens kubeconfig review automatically.")
        .accessibilityIdentifier("rune.add-cluster.provider.recommended")
    }

    func importKubeConfigFromProviderSheet() {
        selectedAddClusterProvider = nil
        viewModel.importKubeConfig()
    }

    func addClusterProviderLocalSetupHelp(_ presentation: AddClusterProviderPresentation) -> some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogControlSpacing) {
            Text(presentation.localCLISetupDescription)
                .runeInterfaceFont(relativeSize: -1)
                .foregroundStyle(.runeSecondary)
                .fixedSize(horizontal: false, vertical: true)

            ForEach(presentation.localCLISetupCommands, id: \.self) { command in
                HStack(alignment: .top, spacing: RuneUILayoutMetrics.dialogControlSpacing) {
                    Text(command)
                        .runeInterfaceFont(relativeSize: -1, design: .monospaced)
                        .textSelection(.enabled)
                        .fixedSize(horizontal: false, vertical: true)
                        .frame(maxWidth: .infinity, alignment: .leading)
                    RuneIconButton("Copy sign-in command", systemImage: "doc.on.doc") {
                        copyToPasteboard(command)
                    }
                    .help("Copy this command to run in Terminal.")
                }
                .runeInsetCard(padding: 8)
            }

            if presentation.provider == .eks {
                Text("Replace <profile> with your SSO profile name before running the sign-in command in Terminal.")
                    .runeInterfaceFont(relativeSize: -2)
                    .foregroundStyle(.runeSecondary)
                    .fixedSize(horizontal: false, vertical: true)
            }

            if let url = presentation.localCLISetupDocumentationURL {
                Link("Installation & sign-in guide", destination: url)
                    .runeInterfaceFont(relativeSize: -1, weight: .semibold)
            }
        }
        .accessibilityIdentifier("rune.add-cluster.provider.local-setup")
    }

    func addClusterProviderFormSection<Content: View>(
        _ title: String,
        @ViewBuilder content: () -> Content
    ) -> some View {
        VStack(alignment: .leading, spacing: RuneSpacing.xs) {
            Text(title)
                .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                .foregroundStyle(.runeSecondary)

            content()
                .runeInsetCard(padding: 12)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    @ViewBuilder
    func providerCredentialInput(_ field: AddClusterProviderField) -> some View {
        switch field.id {
        case .clusterName:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.clusterName)
        case .resourceGroup:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.resourceGroup)
        case .subscription, .profile:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.profileOrSubscription)
        case .region, .location:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.regionOrLocation)
        case .roleARN:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.roleARN)
        case .projectID:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.projectID)
        case .awsAccessKeyID:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.nativeAWSAccessKeyID)
        case .awsSecretAccessKey:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.nativeAWSSecretAccessKey)
        case .awsSessionToken:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.nativeAWSSessionToken)
        case .azureTenantID:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.nativeAKSTenantID)
        case .azureClientID:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.nativeAKSClientID)
        case .azureClientSecret:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.nativeAKSClientSecret)
        case .googleServiceAccountJSON:
            AddClusterProviderCredentialField(field: field) {
                Label("Choose the JSON document with “Import with service account…” below.", systemImage: "doc.badge.plus")
                    .runeInterfaceFont(relativeSize: -1)
                    .foregroundStyle(.runeSecondary)
                    .fixedSize(horizontal: false, vertical: true)
                    .accessibilityValue("Choose with Import with service account below")
            }
        }
    }

    @ViewBuilder
    func providerPrimaryAction(
        _ action: AddClusterProviderAction,
        provider: RuneAddClusterProvider,
        canRunCredentialImport: Bool,
        runHelp: String
    ) -> some View {
        switch action.id {
        case .importKubeconfig:
            Button {
                importKubeConfigFromProviderSheet()
            } label: {
                Label(action.title, systemImage: action.systemImage)
                    .frame(
                        minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                        minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                    )
            }
            .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
            .keyboardShortcut(.defaultAction)
            .help("Import kubeconfig from a file.")
        case .runExternalCLI:
            if let cloudProvider = provider.cloudProvider {
                let idleTitle = isAddClusterProviderAdvancedImportExpanded ? "Add with CLI" : action.title
                Button {
                    viewModel.runCloudKubeConfigImport(cloudCredentialDraft.request(provider: cloudProvider))
                } label: {
                    Label {
                        Text(viewModel.isRunningCloudKubeConfigImport ? "Connecting…" : idleTitle)
                    } icon: {
                        Image(systemName: "icloud.and.arrow.down")
                    }
                    .frame(
                        minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                        minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                    )
                }
                .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
                .keyboardShortcut(isAddClusterProviderAdvancedImportExpanded ? nil : .defaultAction)
                .disabled(!canRunCredentialImport || viewModel.isRunningCloudKubeConfigImport)
                .help(runHelp)
            }
        case .runNativeImport:
            Button {
                runNativeCloudImport(provider)
            } label: {
                Label {
                    Text(viewModel.isRunningCloudKubeConfigImport ? "Importing" : action.title)
                } icon: {
                    Image(systemName: action.systemImage)
                }
                .frame(
                    minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                    minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                )
            }
            .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
            .keyboardShortcut(.defaultAction)
            .disabled(!canRunCredentialImport || viewModel.isRunningCloudKubeConfigImport)
            .help(runHelp)
        case .connectNativeCredentials, .chooseServiceAccountJSON:
            if action.id == .chooseServiceAccountJSON,
               !RuneExternalCommandPolicy.allowsExternalCommands {
                Button {
                    chooseAndRunNativeGKEImport()
                } label: {
                    Label(
                        viewModel.isRunningCloudKubeConfigImport ? "Importing" : action.title,
                        systemImage: action.systemImage
                    )
                    .frame(
                        minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                        minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                    )
                }
                .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
                .keyboardShortcut(.defaultAction)
                .disabled(!canRunCredentialImport || viewModel.isRunningCloudKubeConfigImport)
                .help(runHelp)
            } else {
                nativeCloudAuthConnectButton(provider)
            }
        case .copyExternalCommand,
             .copyLocalSetupCommand,
             .refreshContexts,
             .runAuthDoctor,
             .disconnectNativeCredentials:
            EmptyView()
        }
    }

    func runNativeCloudImport(_ provider: RuneAddClusterProvider) {
        switch provider {
        case .eks:
            let request = cloudCredentialDraft.request(provider: .eks)
            let accessKeyID = cloudCredentialDraft.nativeAWSAccessKeyID
            let secretAccessKey = cloudCredentialDraft.nativeAWSSecretAccessKey
            let sessionToken = cloudCredentialDraft.nativeAWSSessionToken
            cloudCredentialDraft.nativeAWSAccessKeyID = ""
            cloudCredentialDraft.nativeAWSSecretAccessKey = ""
            cloudCredentialDraft.nativeAWSSessionToken = ""
            viewModel.runNativeEKSClusterImport(
                clusterName: request.clusterName,
                region: request.regionOrLocation,
                accessKeyID: accessKeyID,
                secretAccessKey: secretAccessKey,
                sessionToken: sessionToken
            )
        case .aks:
            let request = cloudCredentialDraft.request(provider: .aks)
            let tenantID = cloudCredentialDraft.nativeAKSTenantID
            let clientID = cloudCredentialDraft.nativeAKSClientID
            let clientSecret = cloudCredentialDraft.nativeAKSClientSecret
            cloudCredentialDraft.nativeAKSClientSecret = ""
            viewModel.runNativeAKSClusterImport(
                subscriptionID: request.profileOrSubscription,
                resourceGroup: request.resourceGroup,
                clusterName: request.clusterName,
                tenantID: tenantID,
                clientID: clientID,
                clientSecret: clientSecret
            )
        case .gke:
            chooseAndRunNativeGKEImport()
        case .local:
            break
        }
    }

    func chooseAndRunNativeGKEImport() {
        let request = cloudCredentialDraft.request(provider: .gke)
        viewModel.chooseAndRunNativeGKEClusterImport(
            projectID: request.projectID,
            location: request.regionOrLocation,
            clusterName: request.clusterName
        )
    }

    @ViewBuilder
    func nativeCloudAuthConnectButton(_ provider: RuneAddClusterProvider) -> some View {
        switch provider {
        case .eks:
            Button {
                guard let nativeContext = selectedAddClusterNativeContextOption else { return }
                let accessKeyID = cloudCredentialDraft.nativeAWSAccessKeyID
                let secretAccessKey = cloudCredentialDraft.nativeAWSSecretAccessKey
                let sessionToken = cloudCredentialDraft.nativeAWSSessionToken
                cloudCredentialDraft.nativeAWSAccessKeyID = ""
                cloudCredentialDraft.nativeAWSSecretAccessKey = ""
                cloudCredentialDraft.nativeAWSSessionToken = ""
                viewModel.connectEKSNativeAuth(
                    request: nativeContext.request,
                    accessKeyID: accessKeyID,
                    secretAccessKey: secretAccessKey,
                    sessionToken: sessionToken
                )
            } label: {
                Label(viewModel.isConnectingNativeKubernetesAuth ? "Connecting" : "Connect", systemImage: "key.fill")
                    .frame(
                        minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                        minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                    )
            }
            .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
            .keyboardShortcut(.defaultAction)
            .disabled(
                selectedAddClusterNativeContextOption == nil
                    || !cloudCredentialDraft.hasNativeAWSCredentials
                    || viewModel.isConnectingNativeKubernetesAuth
            )
            .help("Bind AWS credentials to the compatible imported EKS context selected above.")
        case .aks:
            Button {
                guard let nativeContext = selectedAddClusterNativeContextOption else { return }
                let secret = cloudCredentialDraft.nativeAKSClientSecret
                cloudCredentialDraft.nativeAKSClientSecret = ""
                viewModel.connectAKSNativeAuth(request: nativeContext.request, clientSecret: secret)
            } label: {
                Label(viewModel.isConnectingNativeKubernetesAuth ? "Connecting" : "Connect", systemImage: "key.fill")
                    .frame(
                        minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                        minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                    )
            }
            .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
            .keyboardShortcut(.defaultAction)
            .disabled(
                selectedAddClusterNativeContextOption == nil
                    || !cloudCredentialDraft.hasNativeAKSClientSecret
                    || viewModel.isConnectingNativeKubernetesAuth
            )
            .help("Bind an Azure service-principal secret to the compatible imported AKS context selected above.")
        case .gke:
            Button {
                guard let nativeContext = selectedAddClusterNativeContextOption else { return }
                viewModel.chooseAndConnectGKENativeAuth(request: nativeContext.request)
            } label: {
                Label(viewModel.isConnectingNativeKubernetesAuth ? "Connecting" : "Choose JSON…", systemImage: "key.fill")
                    .frame(
                        minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                        minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                    )
            }
            .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
            .keyboardShortcut(.defaultAction)
            .disabled(selectedAddClusterNativeContextOption == nil || viewModel.isConnectingNativeKubernetesAuth)
            .help("Choose and bind a Google service-account JSON file to the compatible imported GKE context selected above.")
        case .local:
            EmptyView()
        }
    }

    func providerCredentialCommand(_ provider: RuneAddClusterProvider, canRunCredentialImport: Bool) -> String {
        guard canRunCredentialImport, let cloudProvider = provider.cloudProvider else {
            return provider.command
        }
        return viewModel.cloudKubeConfigCommandPreview(for: cloudCredentialDraft.request(provider: cloudProvider))
    }

    func canRunProviderCredentialImport(_ provider: RuneAddClusterProvider) -> Bool {
        guard let cloudProvider = provider.cloudProvider else { return false }
        return cloudCredentialDraft.hasRequiredFields(for: cloudProvider)
    }

    func providerCredentialRunHelp(
        _ provider: RuneAddClusterProvider,
        canRunCredentialImport: Bool
    ) -> String {
        guard let cloudProvider = provider.cloudProvider else {
            return "Run the provider CLI locally, validate kubeconfig, and refresh contexts."
        }
        if viewModel.isRunningCloudKubeConfigImport {
            return "Provider import is already running."
        }
        if canRunCredentialImport {
            return "Run the provider CLI locally in the background, then review and add its kubeconfig automatically."
        }
        if let missingFields = cloudCredentialDraft.missingRequiredFieldSummary(for: cloudProvider) {
            return "Enter \(missingFields) to run provider import."
        }
        return "Run the provider CLI locally, validate kubeconfig, and refresh contexts."
    }

    func copyToPasteboard(_ value: String) {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(value, forType: .string)
    }
}
