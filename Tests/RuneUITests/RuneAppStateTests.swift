import XCTest
@testable import RuneCore
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
}

@MainActor
private final class RecordingPortForwardBrowserOpener: PortForwardBrowserOpening {
    private(set) var openedURLs: [URL] = []

    func open(_ url: URL) {
        openedURLs.append(url)
    }
}
