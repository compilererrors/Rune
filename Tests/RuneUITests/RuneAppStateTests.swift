import XCTest
@testable import RuneCore
@testable import RuneDiagnostics
@testable import RuneExport
@testable import RuneUI

final class RuneAppStateTests: XCTestCase {
    @MainActor
    func testOverviewClusterUsageCanUpdateWithoutReplacingOverviewSnapshot() {
        let state = RuneAppState()
        state.setOverviewSnapshot(
            pods: [],
            deploymentsCount: 3,
            servicesCount: 2,
            ingressesCount: 1,
            configMapsCount: 4,
            cronJobsCount: 5,
            nodesCount: 6,
            clusterCPUPercent: nil,
            clusterMemoryPercent: nil,
            events: []
        )

        state.setOverviewClusterUsage(cpuPercent: 17, memoryPercent: 42)

        XCTAssertEqual(state.overviewClusterCPUPercent, 17)
        XCTAssertEqual(state.overviewClusterMemoryPercent, 42)
        XCTAssertEqual(state.overviewDeploymentsCount, 3)
        XCTAssertEqual(state.overviewServicesCount, 2)
        XCTAssertEqual(state.overviewNodesCount, 6)
    }

    @MainActor
    func testNamespaceOptionsAreAlphabetical() {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "cluster")
        state.setNamespaces(["zeta", "default", "Alpha", "beta"])
        state.selectedNamespace = "zeta"
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.namespaceOptions, ["Alpha", "beta", "default", "zeta"])
    }

    @MainActor
    func testContextMenuOptionsAreAlphabeticalWithoutFavoriteGrouping() {
        let state = RuneAppState()
        state.setContexts([
            KubeContext(name: "prod"),
            KubeContext(name: "alpha"),
            KubeContext(name: "Beta")
        ])
        let viewModel = RuneAppViewModel(state: state)
        state.setFavoriteContextNames(["prod"])

        XCTAssertEqual(viewModel.contextMenuOptions.map(\.name), ["alpha", "Beta", "prod"])
        XCTAssertEqual(viewModel.visibleContexts.map(\.name), ["prod", "alpha", "Beta"])
    }

    @MainActor
    func testHistoryBackAndForwardWorksAfterFirstTrackedNavigation() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertFalse(viewModel.canNavigateBack)

        viewModel.setSection(.workloads)

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertTrue(viewModel.canNavigateBack)

        viewModel.navigateBack()

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertFalse(viewModel.canNavigateBack)
        XCTAssertTrue(viewModel.canNavigateForward)

        viewModel.navigateForward()

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertTrue(viewModel.canNavigateBack)
        XCTAssertFalse(viewModel.canNavigateForward)
    }

    @MainActor
    func testCommandPaletteCompositeNavigationKeepsInitialBackTarget() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let item = CommandPaletteItem(
            id: "kind:service",
            title: "Services",
            subtitle: "Networking",
            symbolName: "network",
            action: .resourceKind(section: .networking, kind: .service)
        )

        XCTAssertEqual(state.selectedSection, .overview)

        viewModel.executeCommandPaletteItem(item)

        XCTAssertEqual(state.selectedSection, .networking)
        XCTAssertEqual(state.selectedWorkloadKind, .service)
        XCTAssertTrue(viewModel.canNavigateBack)

        viewModel.navigateBack()

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertFalse(viewModel.canNavigateBack)
        XCTAssertTrue(viewModel.canNavigateForward)
    }

    @MainActor
    func testWorkloadKindSwitchDefersDetailsWhenSnapshotReloadIsNeeded() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod

        viewModel.setWorkloadKind(.deployment)

        XCTAssertEqual(state.selectedWorkloadKind, .deployment)
        XCTAssertFalse(state.isLoadingResourceDetails)
    }

    @MainActor
    func testCompositeSectionAndKindNavigationDefersDetailsUntilRefreshCompletes() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "synthetic")
        state.selectedNamespace = "default"
        state.setServices([
            ServiceSummary(
                name: "sample-service",
                namespace: "default",
                type: "ClusterIP",
                clusterIP: "10.0.0.10"
            )
        ])

        viewModel.setSection(.networking)
        viewModel.setWorkloadKind(.service)

        XCTAssertEqual(state.selectedSection, .networking)
        XCTAssertEqual(state.selectedWorkloadKind, .service)
        XCTAssertFalse(state.isLoadingResourceDetails)
    }

    @MainActor
    func testWorkloadKindNavigationSkipsDetailsForSectionsWithoutInspectors() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedSection = .overview
        state.isLoadingResourceDetails = true

        viewModel.setWorkloadKind(.pod)

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertFalse(state.isLoadingResourceDetails)
    }

    @MainActor
    func testEmptySelectionDoesNotStartInspectorDetailsLoad() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.isLoadingResourceDetails = true

        viewModel.selectPod(nil)

        XCTAssertNil(state.selectedPod)
        XCTAssertFalse(state.isLoadingResourceDetails)
    }

    @MainActor
    func testStoppingPortForwardMarksStartingSessionStoppedImmediately() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let session = PortForwardSession(
            id: "pf-1",
            contextName: "fake",
            namespace: "default",
            targetKind: .pod,
            targetName: "api-0",
            localPort: 8080,
            remotePort: 80,
            address: "127.0.0.1",
            status: .starting,
            lastMessage: "Starting"
        )
        state.isStartingPortForward = true
        state.upsertPortForwardSession(session)

        viewModel.stopPortForward(session)

        XCTAssertEqual(state.portForwardSessions.first?.status, .stopped)
        XCTAssertEqual(state.portForwardSessions.first?.lastMessage, "Port-forward stopped.")
        XCTAssertFalse(state.isStartingPortForward)
    }

    @MainActor
    func testClearingPortForwardSessionsOnlyRemovesInactiveRows() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let active = PortForwardSession(
            id: "pf-active",
            contextName: "fake",
            namespace: "default",
            targetKind: .pod,
            targetName: "api-0",
            localPort: 8080,
            remotePort: 80,
            address: "127.0.0.1",
            status: .active
        )
        let stopped = PortForwardSession(
            id: "pf-stopped",
            contextName: "fake",
            namespace: "default",
            targetKind: .pod,
            targetName: "api-0",
            localPort: 8081,
            remotePort: 80,
            address: "127.0.0.1",
            status: .stopped
        )
        let failed = PortForwardSession(
            id: "pf-failed",
            contextName: "fake",
            namespace: "default",
            targetKind: .pod,
            targetName: "api-0",
            localPort: 8082,
            remotePort: 80,
            address: "127.0.0.1",
            status: .failed
        )
        let otherNamespaceFailed = PortForwardSession(
            id: "pf-other-namespace",
            contextName: "fake",
            namespace: "other",
            targetKind: .pod,
            targetName: "api-0",
            localPort: 8083,
            remotePort: 80,
            address: "127.0.0.1",
            status: .failed
        )
        state.setPortForwardSessions([active, stopped, failed, otherNamespaceFailed])

        viewModel.clearPortForwardSession(active)

        XCTAssertEqual(state.portForwardSessions.map(\.id), ["pf-active", "pf-stopped", "pf-failed", "pf-other-namespace"])

        viewModel.clearPortForwardSession(stopped)

        XCTAssertEqual(state.portForwardSessions.map(\.id), ["pf-active", "pf-failed", "pf-other-namespace"])

        viewModel.clearInactivePortForwardSessions(targetKind: .pod, targetName: "api-0", namespace: "default")

        XCTAssertEqual(state.portForwardSessions.map(\.id), ["pf-active", "pf-other-namespace"])

        viewModel.clearInactivePortForwardSessions()

        XCTAssertEqual(state.portForwardSessions.map(\.id), ["pf-active"])
    }

    @MainActor
    func testOpenPortForwardInBrowserOpensActiveLocalURL() {
        let state = RuneAppState()
        let browserOpener = RecordingPortForwardBrowserOpener()
        let viewModel = RuneAppViewModel(state: state, portForwardBrowserOpener: browserOpener)
        let session = PortForwardSession(
            id: "pf-1",
            contextName: "fake",
            namespace: "default",
            targetKind: .service,
            targetName: "web",
            localPort: 8080,
            remotePort: 80,
            address: "0.0.0.0",
            status: .active,
            lastMessage: "Connected"
        )

        XCTAssertEqual(session.browserURL?.absoluteString, "http://127.0.0.1:8080/")

        viewModel.openPortForwardInBrowser(session)

        XCTAssertEqual(browserOpener.openedURLs.map(\.absoluteString), ["http://127.0.0.1:8080/"])
        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testOpenPortForwardInBrowserRejectsDisconnectedSession() {
        let state = RuneAppState()
        let browserOpener = RecordingPortForwardBrowserOpener()
        let viewModel = RuneAppViewModel(state: state, portForwardBrowserOpener: browserOpener)
        let session = PortForwardSession(
            id: "pf-1",
            contextName: "fake",
            namespace: "default",
            targetKind: .pod,
            targetName: "api-0",
            localPort: 8080,
            remotePort: 80,
            address: "127.0.0.1",
            status: .starting,
            lastMessage: "Starting"
        )

        viewModel.openPortForwardInBrowser(session)

        XCTAssertTrue(browserOpener.openedURLs.isEmpty)
        XCTAssertEqual(state.lastError, "Invalid input: Port-forward is not connected yet.")
    }

    @MainActor
    func testCancelledLogExportDoesNotShowGlobalError() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, exporter: CancelledFileExporter())
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.setPodLogs("line\n")

        viewModel.saveCurrentLogs()

        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testUserCancelledErrorsDoNotCreateGlobalNotice() {
        let state = RuneAppState()

        state.setError(RuneError.userCancelled)

        XCTAssertNil(state.lastError)
        XCTAssertNil(state.activeNotice)
    }

    @MainActor
    func testGlobalErrorsBecomeStructuredNotices() {
        let state = RuneAppState()

        state.setError(RuneError.invalidInput(message: "Choose a namespace."))

        XCTAssertEqual(state.lastError, "Invalid input: Choose a namespace.")
        XCTAssertEqual(state.activeNotice?.severity, .warning)
        XCTAssertEqual(state.activeNotice?.title, "Check the action")

        state.setError(RuneError.commandFailed(command: "kubectl get pods", message: "forbidden"))

        XCTAssertEqual(state.activeNotice?.severity, .error)
        XCTAssertEqual(state.activeNotice?.title, "Kubernetes command failed")

        state.clearError()

        XCTAssertNil(state.lastError)
        XCTAssertNil(state.activeNotice)
    }

    @MainActor
    func testApplyYAMLRequiresUnsavedEdits() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "fake")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: Pod
            metadata:
              name: api-0
            """
        )

        viewModel.requestApplySelectedResourceYAML()

        XCTAssertNil(viewModel.pendingWriteAction)

        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: Pod
            metadata:
              name: api-0
              labels:
                app: api
            """
        )
        viewModel.requestApplySelectedResourceYAML()

        XCTAssertNotNil(viewModel.pendingWriteAction)
    }

    @MainActor
    func testApplyYAMLRejectsValidationErrorsBeforeConfirm() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "fake")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: Pod
            metadata:
              name: api-0
            """
        )
        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: Pod
            metadata:
              name: api-0
              labels:
                app: api
            """
        )
        state.setResourceYAMLValidationIssues([
            YAMLValidationIssue(source: .syntax, severity: .error, message: "bad yaml")
        ])

        viewModel.requestApplySelectedResourceYAML()

        XCTAssertNil(viewModel.pendingWriteAction)
        XCTAssertEqual(state.lastError, "Invalid input: Fix YAML errors before applying.")
    }

    @MainActor
    func testSessionLogCacheKeepsReadSegmentsWithResourceBreaks() {
        let state = RuneAppState()
        let firstDate = Date(timeIntervalSince1970: 1_776_000_000)
        let secondDate = Date(timeIntervalSince1970: 1_776_000_030)

        state.appendPodLogRead(
            "first line\n",
            contextName: "demo-cluster",
            namespace: "demo-namespace",
            podName: "api-0",
            loadedAt: firstDate
        )
        state.appendPodLogRead(
            "second line\n",
            contextName: "demo-cluster",
            namespace: "demo-namespace",
            podName: "api-0",
            loadedAt: secondDate
        )

        XCTAssertTrue(state.podLogs.contains("Pod  demo-namespace/api-0"))
        XCTAssertTrue(state.podLogs.contains("Context: demo-cluster"))
        XCTAssertTrue(state.podLogs.contains("first line"))
        XCTAssertTrue(state.podLogs.contains("second line"))
        XCTAssertGreaterThanOrEqual(state.podLogs.components(separatedBy: "────────────────").count, 5)

        state.setPodLogs("")
        state.showCachedPodLogs(contextName: "demo-cluster", namespace: "demo-namespace", podName: "api-0")
        XCTAssertTrue(state.podLogs.contains("first line"))
        XCTAssertTrue(state.podLogs.contains("second line"))
    }

    @MainActor
    func testUnifiedLogsCanBeScopedToSelectedPods() {
        let result = RuneAppViewModel.scopedUnifiedLogResult(
            mergedText: """
            [api-0] first
            [api-1] second
            [api-0] third
            """,
            podNames: ["api-0", "api-1"],
            selectedPodNames: ["api-0"]
        )

        XCTAssertEqual(result.podNames, ["api-0"])
        XCTAssertTrue(result.mergedText.contains("[api-0] first"))
        XCTAssertTrue(result.mergedText.contains("[api-0] third"))
        XCTAssertFalse(result.mergedText.contains("[api-1] second"))
    }

    @MainActor
    func testResourceYAMLUndoWalksDraftHistoryOneStepAtATime() {
        let state = RuneAppState()
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
            """
        )

        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
              labels:
                app: demo
            """
        )
        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
              labels:
                app: demo
            data:
              enabled: "true"
            """
        )

        XCTAssertTrue(state.canUndoResourceYAMLEdit)

        state.undoResourceYAMLEdit()

        XCTAssertTrue(state.resourceYAML.contains("labels:"))
        XCTAssertFalse(state.resourceYAML.contains("enabled:"))
        XCTAssertTrue(state.canUndoResourceYAMLEdit)

        state.undoResourceYAMLEdit()

        XCTAssertFalse(state.resourceYAML.contains("labels:"))
        XCTAssertFalse(state.canUndoResourceYAMLEdit)

        state.undoResourceYAMLEdit()

        XCTAssertFalse(state.resourceYAML.contains("labels:"))
    }

    @MainActor
    func testPendingApplyMessageIncludesYAMLDiffPreview() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.setSelectedConfigMap(
            ClusterResourceSummary(
                kind: .configMap,
                name: "settings",
                namespace: "default",
                primaryText: "1 data key",
                secondaryText: "2m"
            )
        )
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
            data:
              mode: old
            """
        )
        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
            data:
              mode: new
            """
        )

        viewModel.requestApplySelectedResourceYAML()

        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("YAML diff preview"))
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("-   mode: old"))
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("+   mode: new"))
    }

    @MainActor
    func testPendingApplyMessageIncludesServerDryRunStatus() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.setSelectedConfigMap(
            ClusterResourceSummary(
                kind: .configMap,
                name: "settings",
                namespace: "default",
                primaryText: "1 data key",
                secondaryText: "2m"
            )
        )
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
            data:
              mode: old
            """
        )
        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
            data:
              mode: new
            """
        )

        viewModel.requestApplySelectedResourceYAML()

        XCTAssertEqual(viewModel.pendingWriteDryRunStatus, "Checking with Kubernetes API...")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("Server dry-run:"))
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("Checking with Kubernetes API..."))
    }

    @MainActor
    func testMultipleTerminalSessionsSwitchAndCloseIndependently() {
        let state = RuneAppState()
        let first = PodTerminalSession(
            id: "shell-a",
            contextName: "demo",
            namespace: "default",
            podName: "api-0",
            shell: "sh",
            transcript: "first",
            status: .connected
        )
        let second = PodTerminalSession(
            id: "shell-b",
            contextName: "demo",
            namespace: "default",
            podName: "worker-0",
            shell: "sh",
            transcript: "second",
            status: .connected
        )

        state.setTerminalSession(first)
        state.setTerminalSession(second)

        XCTAssertEqual(state.terminalSessions.map(\.id), ["shell-a", "shell-b"])
        XCTAssertEqual(state.terminalSession?.id, "shell-b")

        state.selectTerminalSession(id: "shell-a")
        state.appendTerminalSessionOutput(id: "shell-b", text: "\nbackground")

        XCTAssertEqual(state.terminalSession?.id, "shell-a")
        XCTAssertEqual(state.terminalSession?.transcript, "first")
        XCTAssertTrue(state.terminalSessions.first(where: { $0.id == "shell-b" })?.transcript.contains("background") == true)

        state.setTerminalSession(nil)

        XCTAssertEqual(state.terminalSessions.map(\.id), ["shell-b"])
        XCTAssertEqual(state.terminalSession?.id, "shell-b")
    }

    @MainActor
    func testTerminalSessionMutationsStayScopedToActiveTab() {
        let state = RuneAppState()
        state.setTerminalSession(PodTerminalSession(
            id: "shell-a",
            contextName: "demo",
            namespace: "default",
            podName: "api-0",
            shell: "sh",
            transcript: "alpha",
            status: .connected
        ))
        state.setTerminalSession(PodTerminalSession(
            id: "shell-b",
            contextName: "demo",
            namespace: "default",
            podName: "worker-0",
            shell: "sh",
            transcript: "beta",
            status: .connected
        ))

        state.selectTerminalSession(id: "shell-a")
        state.clearTerminalSessionTranscript()
        state.updateTerminalSessionStatus(id: "shell-b", status: .failed, exitCode: 137)
        state.appendTerminalSessionCommandEcho(id: "shell-b", command: "date")

        XCTAssertEqual(state.terminalSession?.id, "shell-a")
        XCTAssertEqual(state.terminalSession?.transcript, "")
        let background = state.terminalSessions.first { $0.id == "shell-b" }
        XCTAssertEqual(background?.status, .failed)
        XCTAssertEqual(background?.lastExitCode, 137)
        XCTAssertTrue(background?.transcript.contains("$ date") == true)
    }

    @MainActor
    func testStartingTerminalSessionForConnectedPodReusesExistingTab() {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "benchmark")
        state.selectedNamespace = "default"
        let viewModel = RuneAppViewModel(state: state)
        state.setTerminalSession(PodTerminalSession(
            id: "shell-a",
            contextName: "benchmark",
            namespace: "default",
            podName: "pod-0",
            shell: "sh",
            transcript: "already connected",
            status: .connected
        ))

        viewModel.startTerminalSession(for: PodSummary(name: "pod-0", namespace: "default", status: "Running"))

        XCTAssertEqual(state.terminalSessions.map(\.id), ["shell-a"])
        XCTAssertEqual(state.terminalSession?.id, "shell-a")
        XCTAssertEqual(state.terminalSession?.transcript, "already connected")
    }

    @MainActor
    func testUnifiedLogCacheKeepsPodDeploymentAndServiceReadsSeparate() {
        let state = RuneAppState()
        let loadedAt = Date(timeIntervalSince1970: 1_776_000_000)

        state.appendPodLogRead(
            "pod line",
            contextName: "demo",
            namespace: "default",
            podName: "api",
            loadedAt: loadedAt
        )
        state.appendUnifiedServiceLogRead(
            "deployment line",
            pods: ["api-6f9"],
            contextName: "demo",
            namespace: "default",
            kind: .deployment,
            resourceName: "api",
            loadedAt: loadedAt
        )
        state.appendUnifiedServiceLogRead(
            "service line",
            pods: ["api-6f9"],
            contextName: "demo",
            namespace: "default",
            kind: .service,
            resourceName: "api",
            loadedAt: loadedAt
        )

        XCTAssertTrue(state.cachedLogs(contextName: "demo", namespace: "default", kind: .pod, resourceName: "api").contains("pod line"))
        XCTAssertTrue(state.cachedLogs(contextName: "demo", namespace: "default", kind: .deployment, resourceName: "api").contains("deployment line"))
        XCTAssertFalse(state.cachedLogs(contextName: "demo", namespace: "default", kind: .deployment, resourceName: "api").contains("service line"))
        XCTAssertTrue(state.cachedLogs(contextName: "demo", namespace: "default", kind: .service, resourceName: "api").contains("service line"))
    }

    @MainActor
    func testFavoriteResourcesSortBeforeOtherResources() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.favoriteResources.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let viewModel = RuneAppViewModel(
            state: state,
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: defaults)
        )
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.setConfigMaps([
            ClusterResourceSummary(kind: .configMap, name: "beta", namespace: "default", primaryText: "1 key", secondaryText: "1m"),
            ClusterResourceSummary(kind: .configMap, name: "alpha", namespace: "default", primaryText: "1 key", secondaryText: "1m")
        ])

        XCTAssertEqual(viewModel.visibleConfigMaps.map(\.name), ["alpha", "beta"])

        viewModel.toggleFavoriteResource(kind: .configMap, namespace: "default", name: "beta")

        XCTAssertEqual(viewModel.visibleConfigMaps.map(\.name), ["beta", "alpha"])
    }

    @MainActor
    func testFavoriteResourcesPersistAndAreScopedByKindNamespaceAndName() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.favoriteResourceScope.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)
        state.selectedContext = KubeContext(name: "demo")

        viewModel.toggleFavoriteResource(kind: .pod, namespace: "default", name: "api")
        viewModel.toggleFavoriteResource(kind: .service, namespace: "default", name: "api")
        viewModel.toggleFavoriteResource(kind: .pod, namespace: "other", name: "api")

        XCTAssertTrue(viewModel.isFavoriteResource(kind: .pod, namespace: "default", name: "api"))
        XCTAssertTrue(viewModel.isFavoriteResource(kind: .service, namespace: "default", name: "api"))
        XCTAssertTrue(viewModel.isFavoriteResource(kind: .pod, namespace: "other", name: "api"))
        XCTAssertFalse(viewModel.isFavoriteResource(kind: .deployment, namespace: "default", name: "api"))

        let reloadedState = RuneAppState()
        let reloadedViewModel = RuneAppViewModel(state: reloadedState, contextPreferences: store)
        reloadedState.selectedContext = KubeContext(name: "demo")
        XCTAssertTrue(reloadedViewModel.isFavoriteResource(kind: .pod, namespace: "default", name: "api"))
        XCTAssertTrue(reloadedViewModel.isFavoriteResource(kind: .service, namespace: "default", name: "api"))
        XCTAssertTrue(reloadedViewModel.isFavoriteResource(kind: .pod, namespace: "other", name: "api"))
    }

    @MainActor
    func testFavoriteNamespacesPersistAndSortFirst() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.favoriteNamespaceScope.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)
        state.selectedContext = KubeContext(name: "demo")
        state.setNamespaces(["zeta", "alpha", "bravo"])
        state.selectedNamespace = "alpha"

        XCTAssertEqual(viewModel.namespaceOptions, ["alpha", "bravo", "zeta"])
        viewModel.toggleFavoriteNamespace("zeta")

        XCTAssertTrue(viewModel.isFavoriteNamespace("zeta"))
        XCTAssertEqual(viewModel.namespaceOptions, ["zeta", "alpha", "bravo"])

        let reloadedState = RuneAppState()
        let reloadedViewModel = RuneAppViewModel(state: reloadedState, contextPreferences: store)
        reloadedState.selectedContext = KubeContext(name: "demo")
        reloadedState.setNamespaces(["alpha", "zeta"])
        reloadedState.selectedNamespace = "alpha"
        XCTAssertEqual(reloadedViewModel.namespaceOptions, ["zeta", "alpha"])
    }

    @MainActor
    func testManualNamespacesPersistPerContextAndAppearWithoutListPermission() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.manualNamespaces.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)
        state.selectedContext = KubeContext(name: "demo")
        state.setNamespaces([])

        viewModel.setNamespace("team-a")

        XCTAssertEqual(store.loadManualNamespaces(for: "demo"), ["team-a"])

        let reloadedState = RuneAppState()
        let reloadedViewModel = RuneAppViewModel(state: reloadedState, contextPreferences: store)
        reloadedState.selectedContext = KubeContext(name: "demo")
        reloadedState.selectedNamespace = ""
        reloadedState.setNamespaces([])

        XCTAssertEqual(reloadedViewModel.namespaceOptions, ["team-a"])
    }

    func testPodSummaryExposesContainerNamesForLogSelection() {
        let pod = PodSummary(
            name: "api",
            namespace: "default",
            status: "Running",
            containerNamesLine: "api, metrics , sidecar"
        )

        XCTAssertEqual(pod.containerNames, ["api", "metrics", "sidecar"])
    }

    @MainActor
    func testPodBulkSelectionReconcilesWhenPodListChanges() {
        let state = RuneAppState()
        let api = PodSummary(name: "api-0", namespace: "default", status: "Running")
        let worker = PodSummary(name: "worker-0", namespace: "default", status: "Running")
        let old = PodSummary(name: "old-0", namespace: "default", status: "Failed")
        state.setPods([api, worker, old])

        state.setSelectedPodIDs([api.id, old.id, "missing"])

        XCTAssertEqual(state.selectedPodIDs, [api.id, old.id])

        state.setPods([api, worker])

        XCTAssertEqual(state.selectedPodIDs, [api.id])
    }

    @MainActor
    func testSelectedPodsForBulkActionsFollowVisibleOrderingAndFilter() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "benchmark")
        state.selectedNamespace = "default"
        let pods = [
            PodSummary(name: "worker-0", namespace: "default", status: "Running"),
            PodSummary(name: "api-0", namespace: "default", status: "Running"),
            PodSummary(name: "api-1", namespace: "default", status: "Pending")
        ]
        state.setPods(pods)

        viewModel.togglePodBulkSelection(pods[0])
        viewModel.togglePodBulkSelection(pods[1])
        viewModel.togglePodBulkSelection(pods[2])

        XCTAssertEqual(viewModel.selectedPodsForBulkActions.map(\.name), ["api-0", "api-1", "worker-0"])

        viewModel.setResourceSearchQuery("api")

        XCTAssertEqual(viewModel.selectedPodsForBulkActions.map(\.name), ["api-0", "api-1"])
        XCTAssertEqual(viewModel.selectedPodCount, 3)
    }

    @MainActor
    func testGenericResourceBulkSelectionBuildsDeleteConfirmation() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedWorkloadKind = .configMap
        state.setConfigMaps([
            ClusterResourceSummary(kind: .configMap, name: "settings", namespace: "default", primaryText: "2 keys", secondaryText: "Data"),
            ClusterResourceSummary(kind: .configMap, name: "feature-flags", namespace: "default", primaryText: "1 key", secondaryText: "Data")
        ])

        viewModel.toggleGenericResourceBulkSelection(viewModel.visibleConfigMaps[0])
        viewModel.toggleGenericResourceBulkSelection(viewModel.visibleConfigMaps[1])

        XCTAssertEqual(viewModel.selectedGenericResourceCount, 2)
        XCTAssertTrue(viewModel.areAllVisibleGenericResourcesSelectedForBulkActions)

        viewModel.requestDeleteSelectedGenericResources()

        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Delete 2")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("2 selected resources"))
        XCTAssertTrue(viewModel.pendingWriteActionIsDestructive)
    }

    @MainActor
    func testOperatorResourcesParticipateInSearchAcrossFamilyKindStatusAndMessage() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.setOperatorResources([
            OperatorResourceSummary(
                family: "Flux",
                kind: "Kustomizations",
                apiPath: "/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/default/kustomizations",
                name: "frontend",
                namespace: "default",
                status: "Ready True",
                message: "Applied revision main@sha1"
            ),
            OperatorResourceSummary(
                family: "ArgoCD",
                kind: "Applications",
                apiPath: "/apis/argoproj.io/v1alpha1/namespaces/default/applications",
                name: "payments",
                namespace: "default",
                status: "SyncError False",
                message: "Waiting for health"
            )
        ])

        state.resourceSearchQuery = "syncerror"
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["payments"])

        state.resourceSearchQuery = "revision"
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["frontend"])

        state.resourceSearchQuery = "flux"
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["frontend"])
    }

    @MainActor
    func testOperatorResourceFavoritesSortFirst() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.operatorFavorites.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)
        state.selectedContext = KubeContext(name: "demo")
        state.setOperatorResources([
            OperatorResourceSummary(
                family: "Custom Resources",
                kind: "Widgets",
                apiPath: "/apis/example.io/v1/namespaces/default/widgets",
                name: "alpha",
                namespace: "default",
                status: "Ready",
                message: ""
            ),
            OperatorResourceSummary(
                family: "Custom Resources",
                kind: "Widgets",
                apiPath: "/apis/example.io/v1/namespaces/default/widgets",
                name: "beta",
                namespace: "default",
                status: "Ready",
                message: ""
            )
        ])

        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["alpha", "beta"])
        viewModel.toggleFavoriteOperatorResource(state.operatorResources[1])
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["beta", "alpha"])
    }

    @MainActor
    func testOperatorResourcesArePagedForLargeCRDBrowsing() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.setOperatorResources((0..<45).map { index in
            OperatorResourceSummary(
                family: "Custom Resources",
                kind: "Widgets",
                apiPath: "/apis/example.io/v1/namespaces/default/widgets",
                name: String(format: "widget-%02d", index),
                namespace: "default",
                status: "Ready",
                message: ""
            )
        })

        XCTAssertEqual(viewModel.pagedOperatorResources.count, 40)
        XCTAssertEqual(viewModel.operatorResourcePageSummary, "1-40 of 45")
        XCTAssertTrue(viewModel.canPageOperatorResourcesForward)

        viewModel.pageOperatorResourcesForward()

        XCTAssertEqual(viewModel.pagedOperatorResources.map(\.name), ["widget-40", "widget-41", "widget-42", "widget-43", "widget-44"])
        XCTAssertEqual(viewModel.operatorResourcePageSummary, "41-45 of 45")
        XCTAssertTrue(viewModel.canPageOperatorResourcesBackward)
        XCTAssertFalse(viewModel.canPageOperatorResourcesForward)
    }

    @MainActor
    func testManualNamespaceModeIsExplicitNonBlockingState() {
        let state = RuneAppState()

        state.setManualNamespaceMode(true, warning: "You cannot list namespaces, but you can work in a namespace manually.")

        XCTAssertTrue(state.isManualNamespaceMode)
        XCTAssertEqual(state.namespaceAccessWarning, "You cannot list namespaces, but you can work in a namespace manually.")

        state.clearManualNamespaceMode()
        XCTAssertFalse(state.isManualNamespaceMode)
        XCTAssertNil(state.namespaceAccessWarning)
    }

    @MainActor
    func testLogAndResourceDetailUpdatesExposeTimestamps() {
        let state = RuneAppState()
        let loadedAt = Date(timeIntervalSince1970: 1_700_000_000)

        state.appendPodLogRead(
            "ready",
            contextName: "demo",
            namespace: "default",
            podName: "api",
            loadedAt: loadedAt
        )
        state.setResourceYAML("kind: Pod\nmetadata:\n  name: api\n")

        XCTAssertEqual(state.lastLogUpdatedAt, loadedAt)
        XCTAssertNotNil(state.lastResourceDetailsUpdatedAt)
    }

    @MainActor
    func testLogStreamPauseIsDistinctFromTailMode() {
        let viewModel = RuneAppViewModel(state: RuneAppState())

        XCTAssertFalse(viewModel.isLogTailModeEnabled)
        viewModel.toggleLogStreamPause()
        XCTAssertFalse(viewModel.isLogStreamPaused)

        viewModel.isLogTailModeEnabled = true
        viewModel.toggleLogStreamPause()

        XCTAssertTrue(viewModel.isLogTailModeEnabled)
        XCTAssertTrue(viewModel.isLogStreamPaused)

        viewModel.toggleLogStreamPause()

        XCTAssertTrue(viewModel.isLogTailModeEnabled)
        XCTAssertFalse(viewModel.isLogStreamPaused)

        viewModel.isLogTailModeEnabled = false

        XCTAssertFalse(viewModel.isLogTailModeEnabled)
        XCTAssertFalse(viewModel.isLogStreamPaused)
    }

    @MainActor
    func testCreateManualJobFromCronJobRequiresPendingWriteConfirmation() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.setSelectedCronJob(ClusterResourceSummary(
            kind: .cronJob,
            name: "nightly",
            namespace: "default",
            primaryText: "0 2 * * *",
            secondaryText: "Active"
        ))

        viewModel.createManualJobFromSelectedCronJob()

        XCTAssertEqual(viewModel.pendingWriteActionTitle, "Create a Job from CronJob nightly?")
        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Create Job")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("nightly-manual-"))
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("selected CronJob template"))
    }

    @MainActor
    func testReadOnlyModeBlocksPendingWriteBeforeKubernetesCallAndAuditAppend() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.isReadOnlyMode = true
        viewModel.pendingWriteAction = .createJobFromCronJob(cronJobName: "nightly", jobName: "nightly-manual-1")

        viewModel.confirmPendingWriteAction()

        XCTAssertNil(viewModel.pendingWriteAction)
        XCTAssertEqual(state.lastError, "Read-only mode is on; write actions are blocked.")
        XCTAssertTrue(state.writeAuditLog.isEmpty)
    }

    @MainActor
    func testProductionDestructiveWriteRequiresSecondConfirmation() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "prod")
        state.selectedNamespace = "default"
        viewModel.pendingWriteAction = .delete(kind: .pod, name: "api")

        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Review Production Action")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("Destructive production actions require a second confirmation"))

        viewModel.confirmPendingWriteAction()

        XCTAssertEqual(viewModel.pendingProductionDestructiveConfirmation, .delete(kind: .pod, name: "api"))
        XCTAssertEqual(viewModel.pendingWriteAction, .delete(kind: .pod, name: "api"))
        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Delete")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("Final confirmation required"))
        XCTAssertTrue(state.writeAuditLog.isEmpty)

        viewModel.cancelPendingWriteAction()

        XCTAssertNil(viewModel.pendingProductionDestructiveConfirmation)
        XCTAssertNil(viewModel.pendingWriteAction)
    }

    @MainActor
    func testPendingApplyDiffPreviewTruncatesLargeYAML() {
        let baseline = (0..<40).map { "key\($0): old" }.joined(separator: "\n")
        let edited = (0..<40).map { "key\($0): new" }.joined(separator: "\n")
        let action = PendingWriteAction.apply(kind: .configMap, name: "settings", yaml: edited, baseline: baseline)

        let message = action.message

        XCTAssertTrue(message.contains("YAML diff preview"))
        XCTAssertTrue(message.contains("- key0: old"))
        XCTAssertTrue(message.contains("+ key0: new"))
        XCTAssertTrue(message.contains("… diff truncated"))
    }

    @MainActor
    func testPendingWriteActionBuildsCopyableKubectlCommand() {
        let action = PendingWriteAction.scale(deploymentName: "api service", replicas: 3)

        let command = action.kubectlCommand(contextName: "prod west", namespace: "payments")

        XCTAssertEqual(command, "kubectl --context 'prod west' --namespace payments scale deployment 'api service' --replicas 3")
    }

    @MainActor
    func testWriteAuditLogCapsEntriesAndKeepsNewestFirst() {
        let state = RuneAppState()

        for index in 0..<205 {
            state.appendWriteAuditEntry(
                WriteAuditEntry(
                    action: "Apply YAML",
                    contextName: "demo",
                    namespace: "default",
                    resource: "configmap/settings-\(index)",
                    status: "Succeeded",
                    message: "ok"
                )
            )
        }

        XCTAssertEqual(state.writeAuditLog.count, 200)
        XCTAssertEqual(state.writeAuditLog.first?.resource, "configmap/settings-204")
        XCTAssertEqual(state.writeAuditLog.last?.resource, "configmap/settings-5")
    }

    @MainActor
    func testWriteAuditSearchAndExportUsesVisibleEntries() throws {
        let state = RuneAppState()
        let exporter = RecordingFileExporter()
        let viewModel = RuneAppViewModel(state: state, exporter: exporter)
        state.appendWriteAuditEntry(
            WriteAuditEntry(
                action: "Apply YAML",
                contextName: "demo",
                namespace: "default",
                resource: "configmap/settings",
                status: "Succeeded",
                message: "Write action completed"
            )
        )
        state.appendWriteAuditEntry(
            WriteAuditEntry(
                action: "Delete",
                contextName: "demo",
                namespace: "default",
                resource: "pod/api-0",
                status: "Failed",
                message: "forbidden"
            )
        )

        viewModel.writeAuditSearchQuery = "failed pod/api-0"

        XCTAssertEqual(viewModel.visibleWriteAuditEntries.map(\.resource), ["pod/api-0"])

        viewModel.saveVisibleWriteAuditLog()

        XCTAssertEqual(exporter.saves.count, 1)
        XCTAssertEqual(exporter.saves.first?.allowedFileTypes, ["json"])
        let data = try XCTUnwrap(exporter.saves.first?.data)
        let entries = try JSONDecoder().decode([WriteAuditEntry].self, from: data)
        XCTAssertEqual(entries.map(\.resource), ["pod/api-0"])
    }

    @MainActor
    func testVisibleLogZipExportUsesDisplayedText() throws {
        let state = RuneAppState()
        let exporter = RecordingFileExporter()
        let viewModel = RuneAppViewModel(state: state, exporter: exporter)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")

        viewModel.saveVisibleLogsZip(visibleText: "matched line\n")

        XCTAssertEqual(exporter.saves.count, 1)
        XCTAssertEqual(exporter.saves.first?.allowedFileTypes, ["zip"])
        XCTAssertTrue(exporter.saves.first?.suggestedName.contains("visible-logs") == true)
        XCTAssertGreaterThan(try XCTUnwrap(exporter.saves.first?.data.count), 0)
    }

    func testPodContainerLogArchiveIncludesDeploymentMergedAndContainerFiles() throws {
        let data = try LogArchiveBuilder.buildPodContainerZip(
            records: [
                PodLogArchiveRecord(podName: "api-0", containerName: "app", logs: "ready\nserved request"),
                PodLogArchiveRecord(podName: "api-0", containerName: "sidecar", logs: "proxy ready"),
                PodLogArchiveRecord(podName: "api-1", containerName: nil, logs: "single container")
            ],
            baseName: "deployment-api-pod-logs",
            generatedAt: "20260506T100000Z"
        )

        XCTAssertEqual(Array(data.prefix(2)), [0x50, 0x4b])
        XCTAssertGreaterThan(data.count, 0)
        let archiveText = String(decoding: data, as: UTF8.self)
        XCTAssertTrue(archiveText.contains("deployment-api-pod-logs/merged-20260506T100000Z.log"))
        XCTAssertTrue(archiveText.contains("deployment-api-pod-logs/pods/api-0/app-20260506T100000Z.log"))
        XCTAssertTrue(archiveText.contains("deployment-api-pod-logs/pods/api-0/sidecar-20260506T100000Z.log"))
        XCTAssertTrue(archiveText.contains("deployment-api-pod-logs/pods/api-1/api-1-20260506T100000Z.log"))
        XCTAssertTrue(archiveText.contains("[api-0/app] ready"))
        XCTAssertTrue(archiveText.contains("[api-0/sidecar] proxy ready"))
    }

    @MainActor
    func testSupportBundleIncludesAuthDoctorAndWriteAudit() throws {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.setAuthDoctorChecks([
            RuneHealthCheck(
                id: "namespace-list",
                title: "Namespace list",
                status: .warning,
                message: "Namespace listing is forbidden for /Users/example/.kube/config; manual namespace mode is available."
            )
        ])
        state.appendWriteAuditEntry(
            WriteAuditEntry(
                action: "Apply YAML",
                contextName: "demo",
                namespace: "default",
                resource: "configmap/settings",
                status: "Succeeded",
                message: "Server dry-run passed."
            )
        )

        let request = SupportBundleRequest.snapshot(
            state: state,
            generatedAt: "2026-05-06T00:00:00Z",
            resourceCounts: ["pods": 0],
            selectedResourceKind: "ConfigMap",
            selectedResourceName: "settings"
        )
        let data = try JSONSupportBundleBuilder().buildBundle(from: request)
        let decoded = try JSONDecoder().decode(SupportBundleRequest.self, from: data)

        XCTAssertEqual(decoded.authDoctorChecks.map(\.id), ["namespace-list"])
        XCTAssertEqual(decoded.writeAuditLog.map(\.resource), ["configmap/settings"])
        XCTAssertEqual(decoded.authDoctorChecks.first?.message.contains("<local-path>"), true)
        XCTAssertFalse(String(data: data, encoding: .utf8)?.contains("/Users/") == true)
    }

    @MainActor
    func testAuthDoctorKubeconfigInspectorDetectsManagedAuthWithoutExportingArguments() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        try """
        apiVersion: v1
        clusters:
        - name: demo
          cluster:
            server: https://example.invalid
            certificate-authority-data: REDACTED
            proxy-url: http://proxy.invalid
        users:
        - name: demo
          user:
            exec:
              command: kubelogin
              args:
              - get-token
              - --environment
              - AzurePublicCloud
        contexts:
        - name: demo
          context:
            cluster: demo
            user: demo
        """.write(to: kubeconfig, atomically: true, encoding: .utf8)

        let checks = AuthDoctorKubeconfigInspector().inspect(sources: [KubeConfigSource(url: kubeconfig)])
        let messages = checks.map(\.message).joined(separator: " ")

        XCTAssertTrue(messages.contains("AKS/kubelogin auth hints detected."))
        XCTAssertTrue(messages.contains("Proxy configuration was detected."))
        XCTAssertTrue(messages.contains("Custom certificate authority configuration was detected."))
        XCTAssertFalse(messages.contains("get-token"))
        XCTAssertFalse(messages.contains(kubeconfig.path))
    }
}

@MainActor
private final class RecordingPortForwardBrowserOpener: PortForwardBrowserOpening {
    private(set) var openedURLs: [URL] = []

    func open(_ url: URL) {
        openedURLs.append(url)
    }
}

private struct CancelledFileExporter: FileExporting {
    func save(data: Data, suggestedName: String, allowedFileTypes: [String]) throws -> URL {
        throw FileExportError.userCancelled
    }
}

@MainActor
private final class RecordingFileExporter: FileExporting {
    private(set) var saves: [(data: Data, suggestedName: String, allowedFileTypes: [String])] = []

    func save(data: Data, suggestedName: String, allowedFileTypes: [String]) throws -> URL {
        saves.append((data, suggestedName, allowedFileTypes))
        return URL(fileURLWithPath: "/tmp/\(suggestedName)")
    }
}
