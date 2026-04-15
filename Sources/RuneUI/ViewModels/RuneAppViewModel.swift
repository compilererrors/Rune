import Combine
import Foundation
import RuneCore
import RuneExport
import RuneKube
import RuneSecurity
import RuneStore

public enum PodLogPreset: String, CaseIterable, Identifiable, Sendable {
    case last5Minutes
    case last15Minutes
    case lastHour
    case last6Hours
    case last24Hours
    case last7Days

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .last5Minutes: return "Last 5m"
        case .last15Minutes: return "Last 15m"
        case .lastHour: return "Last 1h"
        case .last6Hours: return "Last 6h"
        case .last24Hours: return "Last 24h"
        case .last7Days: return "Last 7d"
        }
    }

    public var filter: LogTimeFilter {
        switch self {
        case .last5Minutes: return .lastMinutes(5)
        case .last15Minutes: return .lastMinutes(15)
        case .lastHour: return .lastHours(1)
        case .last6Hours: return .lastHours(6)
        case .last24Hours: return .lastHours(24)
        case .last7Days: return .lastDays(7)
        }
    }
}

public enum PendingWriteAction: Sendable {
    case delete(kind: KubeResourceKind, name: String)
    case apply(kind: KubeResourceKind, name: String, yaml: String)
    case scale(deploymentName: String, replicas: Int)
    case rolloutRestart(deploymentName: String)
    case exec(podName: String, command: [String])

    var title: String {
        switch self {
        case let .delete(kind, name):
            return "Delete \(kind.kubectlName) \(name)?"
        case let .apply(kind, name, _):
            return "Apply YAML for \(kind.kubectlName) \(name)?"
        case let .scale(deploymentName, replicas):
            return "Scale deployment \(deploymentName) to \(replicas)?"
        case let .rolloutRestart(deploymentName):
            return "Restart rollout for deployment \(deploymentName)?"
        case let .exec(podName, command):
            return "Run command in pod \(podName)? (\(command.joined(separator: " ")))"
        }
    }

    var message: String {
        switch self {
        case .delete:
            return "This operation mutates cluster state and may be irreversible."
        case .apply:
            return "This applies the current YAML to the active namespace/context."
        case .scale:
            return "Replica count will be changed immediately."
        case .rolloutRestart:
            return "Pods in the deployment will be recreated according to rollout strategy."
        case .exec:
            return "This runs a command inside the selected pod."
        }
    }

    var confirmLabel: String {
        switch self {
        case .delete: return "Delete"
        case .apply: return "Apply"
        case .scale: return "Scale"
        case .rolloutRestart: return "Restart"
        case .exec: return "Run"
        }
    }

    var isDestructive: Bool {
        switch self {
        case .delete: return true
        case .apply, .scale, .rolloutRestart, .exec: return false
        }
    }
}

public struct CommandPaletteItem: Identifiable {
    public enum Action {
        case section(RuneSection)
        case context(KubeContext)
        case namespace(String)
        case pod(PodSummary)
        case deployment(DeploymentSummary)
        case service(ServiceSummary)
        case event(EventSummary)
    }

    public let id: String
    public let title: String
    public let subtitle: String
    public let symbolName: String
    public let action: Action
}

@MainActor
public final class RuneAppViewModel: ObservableObject {
    @Published public private(set) var state: RuneAppState
    @Published public var selectedLogPreset: PodLogPreset = .last15Minutes
    @Published public var includePreviousLogs: Bool = false
    @Published public var pendingWriteAction: PendingWriteAction?
    @Published public var scaleReplicaInput: Int = 1
    @Published public var execCommandInput: String = "printenv"
    @Published public var portForwardLocalPortInput: String = "8080"
    @Published public var portForwardRemotePortInput: String = "8080"
    @Published public var portForwardAddressInput: String = "127.0.0.1"

    private let kubeClient: KubectlClient
    private let bookmarkManager: BookmarkManager
    private let picker: KubeConfigPicking
    private let kubeConfigDiscoverer: KubeConfigDiscovering
    private let store: ResourceStore
    private let exporter: FileExporting
    private let contextPreferences: ContextPreferencesStoring

    private var cancellables: Set<AnyCancellable> = []
    private var hasBootstrapped = false

    public init(
        state: RuneAppState = RuneAppState(),
        kubeClient: KubectlClient = KubectlClient(),
        bookmarkManager: BookmarkManager = BookmarkManager(store: UserDefaultsBookmarkStore()),
        picker: KubeConfigPicking = OpenPanelKubeConfigPicker(),
        kubeConfigDiscoverer: KubeConfigDiscovering = KubeConfigDiscoverer(),
        store: ResourceStore = ResourceStore(),
        exporter: FileExporting = SavePanelExporter(),
        contextPreferences: ContextPreferencesStoring = UserDefaultsContextPreferencesStore()
    ) {
        self.state = state
        self.kubeClient = kubeClient
        self.bookmarkManager = bookmarkManager
        self.picker = picker
        self.kubeConfigDiscoverer = kubeConfigDiscoverer
        self.store = store
        self.exporter = exporter
        self.contextPreferences = contextPreferences

        self.state.setFavoriteContextNames(contextPreferences.loadFavoriteContextNames())

        state.objectWillChange
            .sink { [weak self] _ in
                self?.objectWillChange.send()
            }
            .store(in: &cancellables)

        $selectedLogPreset
            .dropFirst()
            .sink { [weak self] _ in
                self?.reloadLogsForSelection()
            }
            .store(in: &cancellables)

        $includePreviousLogs
            .dropFirst()
            .sink { [weak self] _ in
                self?.reloadLogsForSelection()
            }
            .store(in: &cancellables)
    }

    public var workloadKinds: [KubeResourceKind] {
        [.pod, .deployment, .statefulSet, .daemonSet]
    }

    public var networkingKinds: [KubeResourceKind] {
        [.service, .ingress]
    }

    public var configKinds: [KubeResourceKind] {
        [.configMap, .secret]
    }

    public var storageKinds: [KubeResourceKind] {
        [.node]
    }

    public var writeActionsEnabled: Bool {
        !state.isReadOnlyMode
    }

    public var visibleContexts: [KubeContext] {
        let filtered = state.contexts.filter { context in
            let query = state.contextSearchQuery.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !query.isEmpty else { return true }
            return matches(context.name, query: query)
        }

        return filtered.sorted { lhs, rhs in
            let leftFavorite = state.isFavorite(lhs)
            let rightFavorite = state.isFavorite(rhs)

            if leftFavorite != rightFavorite {
                return leftFavorite && !rightFavorite
            }

            return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
        }
    }

    public var visiblePods: [PodSummary] {
        filtered(state.pods) { pod in
            "\(pod.name) \(pod.status) \(pod.namespace)"
        }
    }

    public var visibleDeployments: [DeploymentSummary] {
        filtered(state.deployments) { deployment in
            "\(deployment.name) \(deployment.namespace) \(deployment.replicaText)"
        }
    }

    public var visibleServices: [ServiceSummary] {
        filtered(state.services) { service in
            "\(service.name) \(service.namespace) \(service.type) \(service.clusterIP)"
        }
    }

    public var visibleStatefulSets: [ClusterResourceSummary] {
        filtered(state.statefulSets) { summaryText(for: $0) }
    }

    public var visibleDaemonSets: [ClusterResourceSummary] {
        filtered(state.daemonSets) { summaryText(for: $0) }
    }

    public var visibleIngresses: [ClusterResourceSummary] {
        filtered(state.ingresses) { summaryText(for: $0) }
    }

    public var visibleConfigMaps: [ClusterResourceSummary] {
        filtered(state.configMaps) { summaryText(for: $0) }
    }

    public var visibleSecrets: [ClusterResourceSummary] {
        filtered(state.secrets) { summaryText(for: $0) }
    }

    public var visibleNodes: [ClusterResourceSummary] {
        filtered(state.nodes) { summaryText(for: $0) }
    }

    public var visibleEvents: [EventSummary] {
        filtered(state.events) { event in
            "\(event.type) \(event.reason) \(event.objectName) \(event.message)"
        }
    }

    public var isProductionContext: Bool {
        guard let context = state.selectedContext?.name.lowercased() else {
            return false
        }

        let markers = ["prod", "production", "live", "critical"]
        return markers.contains { context.contains($0) }
    }

    public var pendingWriteActionTitle: String {
        guard let pendingWriteAction else { return "Confirm write action" }
        return pendingWriteAction.title
    }

    public var pendingWriteActionMessage: String {
        guard let pendingWriteAction else { return "" }
        if state.isReadOnlyMode {
            return "READ-ONLY MODE: stäng av read-only innan write-actions körs."
        }
        if isProductionContext {
            return "PRODUCTION CONTEXT: \(pendingWriteAction.message)"
        }
        return pendingWriteAction.message
    }

    public var pendingWriteActionConfirmLabel: String {
        pendingWriteAction?.confirmLabel ?? "Confirm"
    }

    public var pendingWriteActionIsDestructive: Bool {
        pendingWriteAction?.isDestructive ?? false
    }

    public func bootstrapIfNeeded() {
        guard !hasBootstrapped else { return }
        hasBootstrapped = true
        bootstrap()
    }

    public func bootstrap() {
        Task {
            do {
                var sources = try bookmarkManager.loadKubeConfigSources()

                if sources.isEmpty {
                    for url in kubeConfigDiscoverer.discoverCandidateFiles() {
                        try? bookmarkManager.addKubeConfig(url: url)
                    }
                    sources = try bookmarkManager.loadKubeConfigSources()
                }

                state.setSources(sources)

                guard !sources.isEmpty else {
                    state.setContexts([])
                    state.setPods([])
                    state.setDeployments([])
                    state.setStatefulSets([])
                    state.setDaemonSets([])
                    state.setServices([])
                    state.setIngresses([])
                    state.setConfigMaps([])
                    state.setSecrets([])
                    state.setNodes([])
                    state.setEvents([])
                    state.clearResourceDetails()
                    state.clearError()
                    return
                }

                try await reloadContexts()
            } catch {
                state.setError(error)
            }
        }
    }

    public func importKubeConfig() {
        Task {
            do {
                let files = try picker.pickFiles()
                guard !files.isEmpty else { return }

                for file in files {
                    try bookmarkManager.addKubeConfig(url: file)
                }

                let sources = try bookmarkManager.loadKubeConfigSources()
                state.setSources(sources)
                try await reloadContexts()
            } catch {
                state.setError(error)
            }
        }
    }

    public func reloadContexts() async throws {
        state.isLoading = true
        defer { state.isLoading = false }

        let contexts = try await kubeClient.listContexts(from: state.kubeConfigSources)
        state.setContexts(contexts)

        if let selected = state.selectedContext {
            try await loadResourceSnapshot(context: selected, namespace: state.selectedNamespace)
        }
    }

    public func refreshCurrentView() {
        Task {
            do {
                guard let context = state.selectedContext else { return }
                try await loadResourceSnapshot(context: context, namespace: state.selectedNamespace)
            } catch {
                state.setError(error)
            }
        }
    }

    public func setSection(_ section: RuneSection) {
        state.selectedSection = section
        switch section {
        case .workloads where !workloadKinds.contains(state.selectedWorkloadKind):
            state.selectedWorkloadKind = .pod
        case .networking where !networkingKinds.contains(state.selectedWorkloadKind):
            state.selectedWorkloadKind = .service
        case .config where !configKinds.contains(state.selectedWorkloadKind):
            state.selectedWorkloadKind = .configMap
        case .storage where !storageKinds.contains(state.selectedWorkloadKind):
            state.selectedWorkloadKind = .node
        default:
            break
        }
    }

    public func setWorkloadKind(_ kind: KubeResourceKind) {
        guard kind != .event else { return }
        state.selectedWorkloadKind = kind
        loadResourceDetailsForCurrentSelection()
    }

    public func presentCommandPalette() {
        state.isCommandPalettePresented = true
    }

    public func dismissCommandPalette() {
        state.isCommandPalettePresented = false
    }

    public func setContextSearchQuery(_ query: String) {
        state.contextSearchQuery = query
    }

    public func setResourceSearchQuery(_ query: String) {
        state.resourceSearchQuery = query
    }

    public func setReadOnlyMode(_ value: Bool) {
        state.isReadOnlyMode = value
    }

    public func setContext(_ context: KubeContext) {
        state.selectedContext = context
        state.clearResourceDetails()
        refreshCurrentView()
    }

    public func setNamespace(_ namespace: String) {
        let trimmed = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        state.selectedNamespace = trimmed
        state.clearResourceDetails()
        refreshCurrentView()
    }

    public func toggleFavorite(for context: KubeContext) {
        state.toggleFavoriteContext(named: context.name)
        contextPreferences.saveFavoriteContextNames(state.favoriteContextNames)
    }

    public func selectPod(_ pod: PodSummary?) {
        state.setSelectedPod(pod)
        state.selectedWorkloadKind = .pod
        loadResourceDetailsForCurrentSelection()
    }

    public func selectDeployment(_ deployment: DeploymentSummary?) {
        state.setSelectedDeployment(deployment)
        state.selectedWorkloadKind = .deployment
        if let deployment {
            scaleReplicaInput = max(0, deployment.desiredReplicas)
        }
        loadResourceDetailsForCurrentSelection()
    }

    public func selectService(_ service: ServiceSummary?) {
        state.setSelectedService(service)
        state.selectedWorkloadKind = .service
        loadResourceDetailsForCurrentSelection()
    }

    public func selectEvent(_ event: EventSummary?) {
        state.setSelectedEvent(event)
    }

    public func selectStatefulSet(_ resource: ClusterResourceSummary?) {
        state.setSelectedStatefulSet(resource)
        state.selectedWorkloadKind = .statefulSet
        loadResourceDetailsForCurrentSelection()
    }

    public func selectDaemonSet(_ resource: ClusterResourceSummary?) {
        state.setSelectedDaemonSet(resource)
        state.selectedWorkloadKind = .daemonSet
        loadResourceDetailsForCurrentSelection()
    }

    public func selectIngress(_ resource: ClusterResourceSummary?) {
        state.setSelectedIngress(resource)
        state.selectedWorkloadKind = .ingress
        loadResourceDetailsForCurrentSelection()
    }

    public func selectConfigMap(_ resource: ClusterResourceSummary?) {
        state.setSelectedConfigMap(resource)
        state.selectedWorkloadKind = .configMap
        loadResourceDetailsForCurrentSelection()
    }

    public func selectSecret(_ resource: ClusterResourceSummary?) {
        state.setSelectedSecret(resource)
        state.selectedWorkloadKind = .secret
        loadResourceDetailsForCurrentSelection()
    }

    public func selectNode(_ resource: ClusterResourceSummary?) {
        state.setSelectedNode(resource)
        state.selectedWorkloadKind = .node
        loadResourceDetailsForCurrentSelection()
    }

    public func reloadLogsForSelection() {
        Task {
            do {
                guard let context = state.selectedContext else { return }
                switch state.selectedWorkloadKind {
                case .pod:
                    guard let pod = state.selectedPod else { return }
                    let logs = try await kubeClient.podLogs(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        podName: pod.name,
                        filter: selectedLogPreset.filter,
                        previous: includePreviousLogs
                    )
                    state.setPodLogs(logs)
                case .service:
                    guard let service = state.selectedService else { return }
                    let unified = try await kubeClient.unifiedLogsForService(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        service: service,
                        filter: selectedLogPreset.filter,
                        previous: includePreviousLogs
                    )
                    state.setUnifiedServiceLogs(unified.mergedText, pods: unified.podNames)
                case .deployment:
                    guard let deployment = state.selectedDeployment else { return }
                    let unified = try await kubeClient.unifiedLogsForDeployment(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        deployment: deployment,
                        filter: selectedLogPreset.filter,
                        previous: includePreviousLogs
                    )
                    state.setUnifiedServiceLogs(unified.mergedText, pods: unified.podNames)
                case .statefulSet, .daemonSet, .ingress, .configMap, .secret, .node, .event:
                    return
                }
            } catch {
                state.setError(error)
            }
        }
    }

    public func saveCurrentLogs() {
        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")

            switch state.selectedWorkloadKind {
            case .pod:
                guard let pod = state.selectedPod else { return }
                _ = try exporter.save(
                    data: Data(state.podLogs.utf8),
                    suggestedName: "pod-\(pod.name)-logs-\(timestamp).log",
                    allowedFileTypes: ["log", "txt"]
                )
            case .service:
                guard let service = state.selectedService else { return }
                _ = try exporter.save(
                    data: Data(state.unifiedServiceLogs.utf8),
                    suggestedName: "service-\(service.name)-unified-logs-\(timestamp).log",
                    allowedFileTypes: ["log", "txt"]
                )
            case .deployment:
                guard let deployment = state.selectedDeployment else { return }
                _ = try exporter.save(
                    data: Data(state.unifiedServiceLogs.utf8),
                    suggestedName: "deployment-\(deployment.name)-unified-logs-\(timestamp).log",
                    allowedFileTypes: ["log", "txt"]
                )
            case .statefulSet, .daemonSet, .ingress, .configMap, .secret, .node, .event:
                return
            }
        } catch {
            state.setError(error)
        }
    }

    public func saveVisibleEvents() {
        do {
            guard !visibleEvents.isEmpty else { return }
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")

            let lines = visibleEvents.map { event in
                "[\(event.type)] \(event.reason) • \(event.objectName)\n\(event.message)"
            }
            let payload = lines.joined(separator: "\n\n")

            _ = try exporter.save(
                data: Data(payload.utf8),
                suggestedName: "events-\(state.selectedNamespace)-\(timestamp).txt",
                allowedFileTypes: ["txt", "log"]
            )
        } catch {
            state.setError(error)
        }
    }

    public func saveCurrentResourceYAML() {
        do {
            guard let (kind, name) = currentWritableResource(), !state.resourceYAML.isEmpty else { return }
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")

            _ = try exporter.save(
                data: Data(state.resourceYAML.utf8),
                suggestedName: "\(kind.kubectlName)-\(name)-\(timestamp).yaml",
                allowedFileTypes: ["yaml", "yml"]
            )
        } catch {
            state.setError(error)
        }
    }

    public func requestDeleteSelectedResource() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let (kind, name) = currentDeletableResource() else { return }
        pendingWriteAction = .delete(kind: kind, name: name)
    }

    public func requestApplySelectedResourceYAML() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let (kind, name) = currentWritableResource(), !state.resourceYAML.isEmpty else { return }
        pendingWriteAction = .apply(kind: kind, name: name, yaml: state.resourceYAML)
    }

    public func requestScaleSelectedDeployment() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let deployment = state.selectedDeployment else { return }
        pendingWriteAction = .scale(deploymentName: deployment.name, replicas: max(0, scaleReplicaInput))
    }

    public func requestRolloutRestartSelectedDeployment() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let deployment = state.selectedDeployment else { return }
        pendingWriteAction = .rolloutRestart(deploymentName: deployment.name)
    }

    public func requestExecInSelectedPod() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let pod = state.selectedPod else { return }

        do {
            let command = try parseCommandInput(execCommandInput)
            pendingWriteAction = .exec(podName: pod.name, command: command)
        } catch {
            state.setError(error)
        }
    }

    public func startPortForwardForSelection() {
        Task {
            do {
                guard let context = state.selectedContext else { return }
                let localPort = try parsePort(portForwardLocalPortInput, fieldName: "local port")
                let remotePort = try parsePort(portForwardRemotePortInput, fieldName: "remote port")
                let address = normalizedPortForwardAddress()

                let target: (PortForwardTargetKind, String)
                switch state.selectedWorkloadKind {
                case .pod:
                    guard let pod = state.selectedPod else { return }
                    target = (.pod, pod.name)
                case .service:
                    guard let service = state.selectedService else { return }
                    target = (.service, service.name)
                case .deployment, .statefulSet, .daemonSet, .ingress, .configMap, .secret, .node, .event:
                    throw RuneError.invalidInput(message: "Port-forward stöds just nu för pod eller service.")
                }

                state.isStartingPortForward = true
                defer { state.isStartingPortForward = false }

                let session = try await kubeClient.startPortForward(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    targetKind: target.0,
                    targetName: target.1,
                    localPort: localPort,
                    remotePort: remotePort,
                    address: address
                ) { [weak self] session in
                    Task { @MainActor in
                        self?.state.upsertPortForwardSession(session)
                    }
                }

                if !state.portForwardSessions.contains(where: { $0.id == session.id }) {
                    state.upsertPortForwardSession(session)
                }
                state.selectedSection = .terminal
            } catch {
                state.setError(error)
            }
        }
    }

    public func stopPortForward(_ session: PortForwardSession) {
        Task {
            await kubeClient.stopPortForward(sessionID: session.id)
        }
    }

    public func cancelPendingWriteAction() {
        pendingWriteAction = nil
    }

    public func confirmPendingWriteAction() {
        guard writeActionsEnabled else {
            pendingWriteAction = nil
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let action = pendingWriteAction else { return }
        pendingWriteAction = nil

        Task {
            do {
                guard let context = state.selectedContext else { return }

                switch action {
                case let .delete(kind, name):
                    try await kubeClient.deleteResource(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        kind: kind,
                        name: name
                    )
                case let .apply(_, _, yaml):
                    try await kubeClient.applyYAML(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        yaml: yaml
                    )
                case let .scale(deploymentName, replicas):
                    try await kubeClient.scaleDeployment(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        deploymentName: deploymentName,
                        replicas: replicas
                    )
                case let .rolloutRestart(deploymentName):
                    try await kubeClient.restartDeploymentRollout(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        deploymentName: deploymentName
                    )
                case let .exec(podName, command):
                    state.isExecutingCommand = true
                    defer { state.isExecutingCommand = false }

                    let result = try await kubeClient.execInPod(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        podName: podName,
                        container: nil,
                        command: command
                    )
                    state.setLastExecResult(result)
                    state.selectedSection = .terminal
                    return
                }

                try await loadResourceSnapshot(context: context, namespace: state.selectedNamespace)
            } catch {
                state.setError(error)
            }
        }
    }

    public func commandPaletteItems(query: String) -> [CommandPaletteItem] {
        let sections = RuneSection.allCases.map { section in
            CommandPaletteItem(
                id: "section:\(section.rawValue)",
                title: section.title,
                subtitle: "Switch section",
                symbolName: section.symbolName,
                action: .section(section)
            )
        }

        let contexts = visibleContexts.map { context in
            CommandPaletteItem(
                id: "context:\(context.name)",
                title: context.name,
                subtitle: "Switch context",
                symbolName: state.isFavorite(context) ? "star.fill" : "network",
                action: .context(context)
            )
        }

        let namespaceCandidates = [state.selectedNamespace, "default", "kube-system", "kube-public", "prod"]
        let namespaces = Array(Set(namespaceCandidates))
            .filter { !$0.isEmpty }
            .sorted()
            .map { namespace in
                CommandPaletteItem(
                    id: "namespace:\(namespace)",
                    title: namespace,
                    subtitle: "Switch namespace",
                    symbolName: "square.3.layers.3d",
                    action: .namespace(namespace)
                )
            }

        let pods = visiblePods.prefix(40).map { pod in
            CommandPaletteItem(
                id: "pod:\(pod.id)",
                title: pod.name,
                subtitle: "Open pod",
                symbolName: "cube.box",
                action: .pod(pod)
            )
        }

        let deployments = visibleDeployments.prefix(40).map { deployment in
            CommandPaletteItem(
                id: "deployment:\(deployment.id)",
                title: deployment.name,
                subtitle: "Open deployment",
                symbolName: "shippingbox",
                action: .deployment(deployment)
            )
        }

        let services = visibleServices.prefix(40).map { service in
            CommandPaletteItem(
                id: "service:\(service.id)",
                title: service.name,
                subtitle: "Open service",
                symbolName: "point.3.connected.trianglepath.dotted",
                action: .service(service)
            )
        }

        let events = visibleEvents.prefix(40).map { event in
            CommandPaletteItem(
                id: "event:\(event.id)",
                title: "\(event.reason) (\(event.type))",
                subtitle: event.objectName,
                symbolName: "bolt.badge.clock",
                action: .event(event)
            )
        }

        let allItems = sections + contexts + namespaces + pods + deployments + services + events
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)

        guard !trimmedQuery.isEmpty else {
            return Array(allItems.prefix(160))
        }

        return allItems.filter { item in
            matches("\(item.title) \(item.subtitle)", query: trimmedQuery)
        }
    }

    public func executeCommandPaletteItem(_ item: CommandPaletteItem) {
        switch item.action {
        case let .section(section):
            state.selectedSection = section
        case let .context(context):
            setContext(context)
        case let .namespace(namespace):
            setNamespace(namespace)
        case let .pod(pod):
            state.selectedSection = .workloads
            setWorkloadKind(.pod)
            selectPod(pod)
        case let .deployment(deployment):
            state.selectedSection = .workloads
            setWorkloadKind(.deployment)
            selectDeployment(deployment)
        case let .service(service):
            setSection(.networking)
            setWorkloadKind(.service)
            selectService(service)
        case let .event(event):
            state.selectedSection = .events
            selectEvent(event)
        }

        dismissCommandPalette()
    }

    private func loadResourceSnapshot(context: KubeContext, namespace: String) async throws {
        state.isLoading = true
        defer { state.isLoading = false }

        async let pods = kubeClient.listPods(from: state.kubeConfigSources, context: context, namespace: namespace)
        async let deployments = kubeClient.listDeployments(from: state.kubeConfigSources, context: context, namespace: namespace)
        async let statefulSets = kubeClient.listStatefulSets(from: state.kubeConfigSources, context: context, namespace: namespace)
        async let daemonSets = kubeClient.listDaemonSets(from: state.kubeConfigSources, context: context, namespace: namespace)
        async let services = kubeClient.listServices(from: state.kubeConfigSources, context: context, namespace: namespace)
        async let ingresses = kubeClient.listIngresses(from: state.kubeConfigSources, context: context, namespace: namespace)
        async let configMaps = kubeClient.listConfigMaps(from: state.kubeConfigSources, context: context, namespace: namespace)
        async let secrets = kubeClient.listSecrets(from: state.kubeConfigSources, context: context, namespace: namespace)
        async let nodes = kubeClient.listNodes(from: state.kubeConfigSources, context: context)
        async let events = kubeClient.listEvents(from: state.kubeConfigSources, context: context, namespace: namespace)

        let loadedPods = try await pods
        let loadedDeployments = try await deployments
        let loadedStatefulSets = try await statefulSets
        let loadedDaemonSets = try await daemonSets
        let loadedServices = try await services
        let loadedIngresses = try await ingresses
        let loadedConfigMaps = try await configMaps
        let loadedSecrets = try await secrets
        let loadedNodes = try await nodes
        let loadedEvents = try await events

        store.cachePods(loadedPods, context: context, namespace: namespace)

        state.setPods(loadedPods)
        state.setDeployments(loadedDeployments)
        state.setStatefulSets(loadedStatefulSets)
        state.setDaemonSets(loadedDaemonSets)
        state.setServices(loadedServices)
        state.setIngresses(loadedIngresses)
        state.setConfigMaps(loadedConfigMaps)
        state.setSecrets(loadedSecrets)
        state.setNodes(loadedNodes)
        state.setEvents(loadedEvents)

        if let deployment = state.selectedDeployment {
            scaleReplicaInput = max(0, deployment.desiredReplicas)
        }

        await loadResourceDetailsForCurrentSelectionAsync()
    }

    private func loadResourceDetailsForCurrentSelection() {
        Task {
            await loadResourceDetailsForCurrentSelectionAsync()
        }
    }

    private func loadResourceDetailsForCurrentSelectionAsync() async {
        do {
            guard let context = state.selectedContext else { return }

            switch state.selectedWorkloadKind {
            case .pod:
                guard let pod = state.selectedPod else {
                    state.clearResourceDetails()
                    return
                }

                async let logs = kubeClient.podLogs(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    podName: pod.name,
                    filter: selectedLogPreset.filter,
                    previous: includePreviousLogs
                )

                async let yaml = kubeClient.resourceYAML(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .pod,
                    name: pod.name
                )

                state.setPodLogs(try await logs)
                state.setResourceYAML(try await yaml)
                state.clearUnifiedServiceLogs()

            case .service:
                guard let service = state.selectedService else {
                    state.clearResourceDetails()
                    return
                }

                async let unified = kubeClient.unifiedLogsForService(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    service: service,
                    filter: selectedLogPreset.filter,
                    previous: includePreviousLogs
                )

                async let yaml = kubeClient.resourceYAML(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .service,
                    name: service.name
                )

                let unifiedResult = try await unified
                state.setUnifiedServiceLogs(unifiedResult.mergedText, pods: unifiedResult.podNames)
                state.setResourceYAML(try await yaml)
                state.setPodLogs("")

            case .deployment:
                guard let deployment = state.selectedDeployment else {
                    state.clearResourceDetails()
                    return
                }

                async let unified = kubeClient.unifiedLogsForDeployment(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    deployment: deployment,
                    filter: selectedLogPreset.filter,
                    previous: includePreviousLogs
                )

                async let yaml = kubeClient.resourceYAML(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .deployment,
                    name: deployment.name
                )

                let unifiedResult = try await unified
                state.setUnifiedServiceLogs(unifiedResult.mergedText, pods: unifiedResult.podNames)
                state.setResourceYAML(try await yaml)
                state.setPodLogs("")

            case .statefulSet:
                guard let resource = state.selectedStatefulSet else {
                    state.clearResourceDetails()
                    return
                }

                let yaml = try await kubeClient.resourceYAML(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .statefulSet,
                    name: resource.name
                )

                state.setResourceYAML(yaml)
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

            case .daemonSet:
                guard let resource = state.selectedDaemonSet else {
                    state.clearResourceDetails()
                    return
                }

                let yaml = try await kubeClient.resourceYAML(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .daemonSet,
                    name: resource.name
                )

                state.setResourceYAML(yaml)
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

            case .ingress:
                guard let resource = state.selectedIngress else {
                    state.clearResourceDetails()
                    return
                }

                let yaml = try await kubeClient.resourceYAML(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .ingress,
                    name: resource.name
                )

                state.setResourceYAML(yaml)
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

            case .configMap:
                guard let resource = state.selectedConfigMap else {
                    state.clearResourceDetails()
                    return
                }

                let yaml = try await kubeClient.resourceYAML(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .configMap,
                    name: resource.name
                )

                state.setResourceYAML(yaml)
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

            case .secret:
                guard let resource = state.selectedSecret else {
                    state.clearResourceDetails()
                    return
                }

                let yaml = try await kubeClient.resourceYAML(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .secret,
                    name: resource.name
                )

                state.setResourceYAML(yaml)
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

            case .node:
                guard let resource = state.selectedNode else {
                    state.clearResourceDetails()
                    return
                }

                let yaml = try await kubeClient.resourceYAML(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .node,
                    name: resource.name
                )

                state.setResourceYAML(yaml)
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

            case .event:
                state.clearResourceDetails()
            }
        } catch {
            state.setError(error)
        }
    }

    private func currentWritableResource() -> (KubeResourceKind, String)? {
        switch state.selectedWorkloadKind {
        case .pod:
            guard let pod = state.selectedPod else { return nil }
            return (.pod, pod.name)
        case .deployment:
            guard let deployment = state.selectedDeployment else { return nil }
            return (.deployment, deployment.name)
        case .statefulSet:
            guard let resource = state.selectedStatefulSet else { return nil }
            return (.statefulSet, resource.name)
        case .daemonSet:
            guard let resource = state.selectedDaemonSet else { return nil }
            return (.daemonSet, resource.name)
        case .service:
            guard let service = state.selectedService else { return nil }
            return (.service, service.name)
        case .ingress:
            guard let resource = state.selectedIngress else { return nil }
            return (.ingress, resource.name)
        case .configMap:
            guard let resource = state.selectedConfigMap else { return nil }
            return (.configMap, resource.name)
        case .secret:
            guard let resource = state.selectedSecret else { return nil }
            return (.secret, resource.name)
        case .node:
            guard let resource = state.selectedNode else { return nil }
            return (.node, resource.name)
        case .event:
            return nil
        }
    }

    private func currentDeletableResource() -> (KubeResourceKind, String)? {
        currentWritableResource()
    }

    private func filtered<T>(_ values: [T], text: (T) -> String) -> [T] {
        let query = state.resourceSearchQuery.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty else {
            return values
        }

        return values.filter { value in
            matches(text(value), query: query)
        }
    }

    private func matches(_ text: String, query: String) -> Bool {
        let normalizedText = text.lowercased()
        let tokens = query.lowercased().split(whereSeparator: \.isWhitespace)
        return tokens.allSatisfy { normalizedText.contains($0) }
    }

    private func summaryText(for resource: ClusterResourceSummary) -> String {
        "\(resource.name) \(resource.namespace ?? "") \(resource.primaryText) \(resource.secondaryText)"
    }

    private func parsePort(_ value: String, fieldName: String) throws -> Int {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let port = Int(trimmed), (1...65535).contains(port) else {
            throw RuneError.invalidInput(message: "\(fieldName) måste vara ett nummer mellan 1 och 65535.")
        }
        return port
    }

    private func normalizedPortForwardAddress() -> String {
        let trimmed = portForwardAddressInput.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? "127.0.0.1" : trimmed
    }

    private func parseCommandInput(_ input: String) throws -> [String] {
        let trimmed = input.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            throw RuneError.invalidInput(message: "exec-kommandot får inte vara tomt.")
        }

        var tokens: [String] = []
        var current = ""
        var activeQuote: Character?

        for character in trimmed {
            if activeQuote != nil {
                if character == activeQuote {
                    activeQuote = nil
                } else {
                    current.append(character)
                }
                continue
            }

            if character == "\"" || character == "'" {
                activeQuote = character
                continue
            }

            if character.isWhitespace {
                if !current.isEmpty {
                    tokens.append(current)
                    current = ""
                }
                continue
            }

            current.append(character)
        }

        if let activeQuote {
            throw RuneError.invalidInput(message: "saknar avslutande citationstecken \(activeQuote).")
        }

        if !current.isEmpty {
            tokens.append(current)
        }

        guard !tokens.isEmpty else {
            throw RuneError.invalidInput(message: "exec-kommandot kunde inte tolkas.")
        }

        return tokens
    }
}
