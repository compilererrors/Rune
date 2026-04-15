import AppKit
import RuneCore
import SwiftUI

private enum PodInspectorTab: String, CaseIterable, Identifiable {
    case overview
    case logs
    case exec
    case portForward
    case yaml

    var id: String { rawValue }

    var title: String {
        switch self {
        case .overview: return "Overview"
        case .logs: return "Logs"
        case .exec: return "Exec"
        case .portForward: return "Port Forward"
        case .yaml: return "YAML"
        }
    }
}

private enum ServiceInspectorTab: String, CaseIterable, Identifiable {
    case overview
    case unifiedLogs
    case portForward
    case yaml

    var id: String { rawValue }

    var title: String {
        switch self {
        case .overview: return "Overview"
        case .unifiedLogs: return "Unified Logs"
        case .portForward: return "Port Forward"
        case .yaml: return "YAML"
        }
    }
}

private enum DeploymentInspectorTab: String, CaseIterable, Identifiable {
    case overview
    case unifiedLogs
    case yaml

    var id: String { rawValue }

    var title: String {
        switch self {
        case .overview: return "Overview"
        case .unifiedLogs: return "Unified Logs"
        case .yaml: return "YAML"
        }
    }
}

public struct RuneRootView: View {
    @ObservedObject private var viewModel: RuneAppViewModel

    @State private var namespaceInput: String = "default"
    @State private var podInspectorTab: PodInspectorTab = .overview
    @State private var serviceInspectorTab: ServiceInspectorTab = .overview
    @State private var deploymentInspectorTab: DeploymentInspectorTab = .overview

    public init(viewModel: RuneAppViewModel = RuneAppViewModel()) {
        self.viewModel = viewModel
    }

    public var body: some View {
        ZStack(alignment: .top) {
            background

            NavigationSplitView {
                sidebar
                    .navigationSplitViewColumnWidth(min: 220, ideal: 280, max: 480)
            } content: {
                contentPane
                    .navigationSplitViewColumnWidth(min: 420, ideal: 700, max: 1200)
            } detail: {
                detailPane
                    .navigationSplitViewColumnWidth(min: 320, ideal: 420, max: 820)
            }
            .navigationSplitViewStyle(.balanced)
            .toolbar {
                ToolbarItemGroup(placement: .navigation) {
                    Menu(viewModel.state.selectedContext?.name ?? "No Context") {
                        ForEach(viewModel.visibleContexts) { context in
                            Button(context.name) {
                                viewModel.setContext(context)
                            }
                        }
                    }

                    Menu(viewModel.state.selectedNamespace) {
                        ForEach(namespaceSuggestions, id: \.self) { namespace in
                            Button(namespace) {
                                namespaceInput = namespace
                                viewModel.setNamespace(namespace)
                            }
                        }
                    }
                }

                ToolbarItemGroup(placement: .primaryAction) {
                    Button("Palette") {
                        viewModel.presentCommandPalette()
                    }
                    .keyboardShortcut("k", modifiers: .command)

                    Button("Reload") {
                        viewModel.refreshCurrentView()
                    }
                    .keyboardShortcut("r", modifiers: .command)
                }
            }
            .sheet(isPresented: commandPalettePresentedBinding) {
                CommandPaletteView(viewModel: viewModel)
            }
            .confirmationDialog(
                viewModel.pendingWriteActionTitle,
                isPresented: pendingWriteActionPresentedBinding,
                titleVisibility: .visible
            ) {
                if viewModel.pendingWriteActionIsDestructive {
                    Button(viewModel.pendingWriteActionConfirmLabel, role: .destructive) {
                        viewModel.confirmPendingWriteAction()
                    }
                } else {
                    Button(viewModel.pendingWriteActionConfirmLabel) {
                        viewModel.confirmPendingWriteAction()
                    }
                }

                Button("Cancel", role: .cancel) {
                    viewModel.cancelPendingWriteAction()
                }
            } message: {
                Text(viewModel.pendingWriteActionMessage)
            }
            .onAppear {
                namespaceInput = viewModel.state.selectedNamespace
                viewModel.bootstrapIfNeeded()
            }
            .onChange(of: viewModel.state.selectedNamespace) { _, newValue in
                namespaceInput = newValue
            }

            if viewModel.isProductionContext {
                productionBanner
                    .padding(.top, 6)
                    .transition(.move(edge: .top).combined(with: .opacity))
            }
        }
        .animation(.easeInOut(duration: 0.2), value: viewModel.isProductionContext)
    }

    private var background: some View {
        Color(nsColor: .windowBackgroundColor)
            .ignoresSafeArea()
    }

    private var sidebar: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("Rune")
                .font(.largeTitle.weight(.bold))

            TextField("Search contexts", text: Binding(get: {
                viewModel.state.contextSearchQuery
            }, set: { newValue in
                viewModel.setContextSearchQuery(newValue)
            }))
            .textFieldStyle(.roundedBorder)

            Text("Sections")
                .font(.headline)
                .foregroundStyle(.secondary)

            ForEach(RuneSection.allCases) { section in
                sectionRow(section)
            }

            Divider()
                .overlay(Color(nsColor: .separatorColor))

            Text("Contexts")
                .font(.headline)
                .foregroundStyle(.secondary)

            ScrollView {
                VStack(alignment: .leading, spacing: 8) {
                    ForEach(viewModel.visibleContexts) { context in
                        contextRow(context)
                    }
                }
            }

            Toggle(isOn: Binding(get: {
                viewModel.state.isReadOnlyMode
            }, set: { value in
                viewModel.setReadOnlyMode(value)
            })) {
                Text("Read-only mode")
                    .font(.headline)
            }

            Spacer(minLength: 0)
        }
        .padding(14)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background(.thinMaterial)
    }

    private func sectionRow(_ section: RuneSection) -> some View {
        Button {
            viewModel.setSection(section)
        } label: {
            HStack(spacing: 8) {
                Image(systemName: section.symbolName)
                    .frame(width: 16)
                Text(section.title + "    ⌘" + String(section.commandShortcut))
                    .font(.body.weight(.medium))
                Spacer()
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 7)
            .background(
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .fill(viewModel.state.selectedSection == section ? Color.accentColor.opacity(0.16) : Color.clear)
            )
        }
        .buttonStyle(.plain)
    }

    private func contextRow(_ context: KubeContext) -> some View {
        HStack(spacing: 8) {
            Button {
                viewModel.setContext(context)
            } label: {
                HStack(spacing: 8) {
                    Circle()
                        .fill(viewModel.state.selectedContext == context ? Color.accentColor : Color.gray.opacity(0.6))
                        .frame(width: 7, height: 7)
                    Text(context.name)
                        .font(.body.weight(.medium))
                        .lineLimit(1)
                        .help(context.name)
                    Spacer()
                }
                .padding(.horizontal, 8)
                .padding(.vertical, 6)
                .background(
                    RoundedRectangle(cornerRadius: 8, style: .continuous)
                        .fill(viewModel.state.selectedContext == context ? Color.accentColor.opacity(0.12) : Color.clear)
                )
            }
            .buttonStyle(.plain)

            Button {
                viewModel.toggleFavorite(for: context)
            } label: {
                Image(systemName: viewModel.state.isFavorite(context) ? "star.fill" : "star")
                    .foregroundStyle(viewModel.state.isFavorite(context) ? Color.yellow : Color.gray)
            }
            .buttonStyle(.plain)
        }
    }

    private var contentPane: some View {
        VStack(alignment: .leading, spacing: 12) {
            contentHeader

            switch viewModel.state.selectedSection {
            case .overview:
                overviewPane
            case .workloads:
                workloadsPane
            case .networking:
                networkingPane
            case .config:
                configPane
            case .storage:
                storagePane
            case .events:
                eventsPane
            case .terminal:
                terminalPane
            default:
                sectionPlaceholder
            }
        }
        .padding(16)
    }

    private var contentHeader: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text(viewModel.state.selectedSection.title)
                    .font(.title2.weight(.bold))

                Spacer()

                if let context = viewModel.state.selectedContext {
                    Label(context.name, systemImage: "network")
                        .padding(.horizontal, 10)
                        .frame(height: 36)
                        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 10, style: .continuous))
                        .help(context.name)
                }

                if viewModel.state.isReadOnlyMode {
                    Label("Read-only", systemImage: "lock.fill")
                        .padding(.horizontal, 10)
                        .frame(height: 36)
                        .background(Color.orange.opacity(0.16), in: RoundedRectangle(cornerRadius: 10, style: .continuous))
                }

                if viewModel.state.selectedSection == .events {
                    Button("Save Events") {
                        viewModel.saveVisibleEvents()
                    }
                    .buttonStyle(.bordered)
                }
            }

            HStack(spacing: 10) {
                TextField("Namespace", text: $namespaceInput)
                    .textFieldStyle(.roundedBorder)
                    .frame(maxWidth: 260)

                Button("Apply") {
                    viewModel.setNamespace(namespaceInput)
                }
                .buttonStyle(.bordered)

                TextField("/ filter resources", text: Binding(get: {
                    viewModel.state.resourceSearchQuery
                }, set: { newValue in
                    viewModel.setResourceSearchQuery(newValue)
                }))
                .textFieldStyle(.roundedBorder)

                if viewModel.state.isLoading {
                    ProgressView()
                        .scaleEffect(0.8)
                }
            }
            .controlSize(.large)

            if viewModel.state.selectedSection == .workloads {
                Picker("Kind", selection: Binding(get: {
                    viewModel.state.selectedWorkloadKind
                }, set: { kind in
                    viewModel.setWorkloadKind(kind)
                })) {
                    ForEach(viewModel.workloadKinds) { kind in
                        Text(kind.title).tag(kind)
                    }
                }
                .pickerStyle(.segmented)
                .frame(maxWidth: 420)
            } else if viewModel.state.selectedSection == .networking {
                Picker("Kind", selection: Binding(get: {
                    viewModel.state.selectedWorkloadKind
                }, set: { kind in
                    viewModel.setWorkloadKind(kind)
                })) {
                    ForEach(viewModel.networkingKinds) { kind in
                        Text(kind.title).tag(kind)
                    }
                }
                .pickerStyle(.segmented)
                .frame(maxWidth: 320)
            } else if viewModel.state.selectedSection == .config {
                Picker("Kind", selection: Binding(get: {
                    viewModel.state.selectedWorkloadKind
                }, set: { kind in
                    viewModel.setWorkloadKind(kind)
                })) {
                    ForEach(viewModel.configKinds) { kind in
                        Text(kind.title).tag(kind)
                    }
                }
                .pickerStyle(.segmented)
                .frame(maxWidth: 320)
            } else if viewModel.state.selectedSection == .storage {
                Picker("Kind", selection: Binding(get: {
                    viewModel.state.selectedWorkloadKind
                }, set: { kind in
                    viewModel.setWorkloadKind(kind)
                })) {
                    ForEach(viewModel.storageKinds) { kind in
                        Text(kind.title).tag(kind)
                    }
                }
                .pickerStyle(.segmented)
                .frame(maxWidth: 200)
            }
        }
    }

    private var overviewPane: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 14) {
                overviewStatusBanner

                LazyVGrid(columns: [GridItem(.adaptive(minimum: 160), spacing: 10)], spacing: 10) {
                    overviewStatCard(title: "Pods", value: "\(viewModel.state.pods.count)", symbol: "cube.box.fill", tint: .cyan)
                    overviewStatCard(title: "Deployments", value: "\(viewModel.state.deployments.count)", symbol: "shippingbox.fill", tint: .blue)
                    overviewStatCard(title: "Services", value: "\(viewModel.state.services.count)", symbol: "point.3.connected.trianglepath.dotted", tint: .purple)
                    overviewStatCard(title: "Ingresses", value: "\(viewModel.state.ingresses.count)", symbol: "network", tint: .indigo)
                    overviewStatCard(title: "ConfigMaps", value: "\(viewModel.state.configMaps.count)", symbol: "doc.text.fill", tint: .teal)
                    overviewStatCard(title: "Nodes", value: "\(viewModel.state.nodes.count)", symbol: "server.rack", tint: .gray)
                    overviewStatCard(title: "Events", value: "\(viewModel.state.events.count)", symbol: "bolt.badge.clock.fill", tint: .orange)
                }

                VStack(alignment: .leading, spacing: 8) {
                    Text("Pod Health")
                        .font(.headline)

                    HStack(spacing: 8) {
                        healthBadge(label: "Running", value: podStatusCount("running"), color: .green)
                        healthBadge(label: "Pending", value: podStatusCount("pending"), color: .orange)
                        healthBadge(label: "Failed", value: podStatusCount("failed"), color: .red)
                        healthBadge(label: "Other", value: max(0, viewModel.state.pods.count - podStatusCount("running") - podStatusCount("pending") - podStatusCount("failed")), color: .gray)
                    }
                }
                .padding(12)
                .background(panelFill, in: RoundedRectangle(cornerRadius: 12, style: .continuous))

                VStack(alignment: .leading, spacing: 8) {
                    Text("Recent Events")
                        .font(.headline)

                    if viewModel.state.events.isEmpty {
                        Text("No events loaded")
                            .foregroundStyle(.secondary)
                            .font(.subheadline)
                    } else {
                        ForEach(Array(viewModel.state.events.prefix(8))) { event in
                            HStack(alignment: .top, spacing: 8) {
                                Image(systemName: event.type.lowercased() == "warning" ? "exclamationmark.triangle.fill" : "checkmark.circle.fill")
                                    .foregroundStyle(event.type.lowercased() == "warning" ? .orange : .green)
                                    .frame(width: 16)

                                VStack(alignment: .leading, spacing: 2) {
                                    Text(event.reason + " • " + event.objectName)
                                        .font(.subheadline.weight(.semibold))
                                    Text(event.message)
                                        .font(.footnote)
                                        .foregroundStyle(.secondary)
                                        .lineLimit(2)
                                }
                            }
                            .padding(.vertical, 2)
                        }
                    }
                }
                .padding(12)
                .background(panelFill, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
            }
        }
        .scrollContentBackground(.hidden)
    }

    private var workloadsPane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                List(viewModel.visiblePods) { pod in
                    Button {
                        viewModel.selectPod(pod)
                    } label: {
                        HStack {
                            Text(pod.name)
                                .font(.body.weight(.medium))
                            Spacer()
                            Text(pod.status)
                                .font(.caption.weight(.semibold))
                                .padding(.horizontal, 8)
                                .padding(.vertical, 2)
                                .background(statusColor(for: pod.status).opacity(0.22), in: Capsule())
                                .foregroundStyle(statusColor(for: pod.status))
                        }
                        .padding(.vertical, 3)
                        .background(
                            RoundedRectangle(cornerRadius: 6, style: .continuous)
                                .fill(viewModel.state.selectedPod == pod ? Color.accentColor.opacity(0.12) : Color.clear)
                        )
                    }
                    .buttonStyle(.plain)
                }

            case .deployment:
                List(viewModel.visibleDeployments) { deployment in
                    Button {
                        viewModel.selectDeployment(deployment)
                    } label: {
                        HStack {
                            Text(deployment.name)
                                .font(.body.weight(.medium))
                            Spacer()
                            Text(deployment.replicaText)
                                .font(.caption.weight(.semibold))
                                .padding(.horizontal, 8)
                                .padding(.vertical, 2)
                                .background(Color.blue.opacity(0.22), in: Capsule())
                                .foregroundStyle(.blue)
                        }
                        .padding(.vertical, 3)
                        .background(
                            RoundedRectangle(cornerRadius: 6, style: .continuous)
                                .fill(viewModel.state.selectedDeployment == deployment ? Color.accentColor.opacity(0.12) : Color.clear)
                        )
                    }
                    .buttonStyle(.plain)
                }

            case .statefulSet:
                genericResourceList(viewModel.visibleStatefulSets, selection: viewModel.state.selectedStatefulSet, action: viewModel.selectStatefulSet)

            case .daemonSet:
                genericResourceList(viewModel.visibleDaemonSets, selection: viewModel.state.selectedDaemonSet, action: viewModel.selectDaemonSet)

            case .service, .ingress, .configMap, .secret, .node:
                EmptyView()

            case .event:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
    }

    private var networkingPane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                List(viewModel.visibleServices) { service in
                    Button {
                        viewModel.selectService(service)
                    } label: {
                        HStack {
                            Text(service.name)
                                .font(.body.weight(.medium))
                                .help(service.name)
                            Spacer()
                            Text(service.type)
                                .font(.caption.weight(.semibold))
                                .padding(.horizontal, 8)
                                .padding(.vertical, 2)
                                .background(Color.purple.opacity(0.22), in: Capsule())
                                .foregroundStyle(.purple)
                        }
                        .padding(.vertical, 3)
                        .background(
                            RoundedRectangle(cornerRadius: 6, style: .continuous)
                                .fill(viewModel.state.selectedService == service ? Color.accentColor.opacity(0.12) : Color.clear)
                        )
                    }
                    .buttonStyle(.plain)
                }

            case .ingress:
                genericResourceList(viewModel.visibleIngresses, selection: viewModel.state.selectedIngress, action: viewModel.selectIngress)

            default:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
    }

    private var configPane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .configMap:
                genericResourceList(viewModel.visibleConfigMaps, selection: viewModel.state.selectedConfigMap, action: viewModel.selectConfigMap)

            case .secret:
                genericResourceList(viewModel.visibleSecrets, selection: viewModel.state.selectedSecret, action: viewModel.selectSecret)

            default:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
    }

    private var storagePane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .node:
                genericResourceList(viewModel.visibleNodes, selection: viewModel.state.selectedNode, action: viewModel.selectNode)

            default:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
    }

    private var eventsPane: some View {
        List(viewModel.visibleEvents) { event in
            Button {
                viewModel.selectEvent(event)
            } label: {
                VStack(alignment: .leading, spacing: 4) {
                    HStack {
                        Text(event.reason)
                            .font(.subheadline.weight(.bold))
                        Spacer()
                        Text(event.type)
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(event.type.lowercased() == "warning" ? .orange : .green)
                    }

                    Text(event.objectName)
                        .font(.caption.weight(.medium))
                        .foregroundStyle(.secondary)

                    Text(event.message)
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                        .lineLimit(2)
                }
                .padding(.vertical, 4)
                .background(
                    RoundedRectangle(cornerRadius: 6, style: .continuous)
                        .fill(viewModel.state.selectedEvent == event ? Color.accentColor.opacity(0.12) : Color.clear)
                )
            }
            .buttonStyle(.plain)
        }
        .scrollContentBackground(.hidden)
    }

    private var sectionPlaceholder: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(viewModel.state.selectedSection.title + " is being implemented")
                .font(.title3.weight(.bold))
            Text("Flow and shortcuts are already wired so section switching feels instant.")
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .padding(20)
        .background(panelFill, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
    }

    private var detailPane: some View {
        VStack(alignment: .leading, spacing: 12) {
            switch viewModel.state.selectedSection {
            case .overview:
                overviewDetails
            case .workloads:
                workloadDetails
            case .networking:
                networkingDetails
            case .config:
                configDetails
            case .storage:
                storageDetails
            case .events:
                eventDetails
            case .terminal:
                terminalDetails
            default:
                Text("Details for \(viewModel.state.selectedSection.title) will appear here.")
                    .foregroundStyle(.secondary)
            }

            if let error = viewModel.state.lastError {
                Text(error)
                    .font(.footnote)
                    .foregroundStyle(.red)
                    .padding(.top, 8)
            }
        }
        .padding(16)
        .background(.regularMaterial)
    }

    private var overviewDetails: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Overview")
                .font(.title2.weight(.bold))

            VStack(alignment: .leading, spacing: 8) {
                Label("Context: \(viewModel.state.selectedContext?.name ?? "-")", systemImage: "network")
                Label("Namespace: \(viewModel.state.selectedNamespace)", systemImage: "square.stack.3d.up")
                Label("Mode: \(viewModel.state.isReadOnlyMode ? "Read-only" : "Read/Write")", systemImage: "lock.shield")
            }
            .font(.subheadline.weight(.medium))

            HStack(spacing: 10) {
                Button("Open Workloads") {
                    viewModel.setSection(.workloads)
                }

                Button("Open Events") {
                    viewModel.setSection(.events)
                }

                Button("Reload") {
                    viewModel.refreshCurrentView()
                }
            }

            Spacer(minLength: 0)
        }
    }

    private var workloadDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                podDetails
            case .deployment:
                deploymentDetails
            case .statefulSet:
                genericResourceDetails(resource: viewModel.state.selectedStatefulSet)
            case .daemonSet:
                genericResourceDetails(resource: viewModel.state.selectedDaemonSet)
            case .service, .ingress, .configMap, .secret, .node:
                EmptyView()
            case .event:
                EmptyView()
            }
        }
    }

    private var networkingDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                serviceDetails
            case .ingress:
                genericResourceDetails(resource: viewModel.state.selectedIngress)
            default:
                Text("Select a networking resource")
                    .foregroundStyle(.secondary)
            }
        }
    }

    private var configDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .configMap:
                genericResourceDetails(resource: viewModel.state.selectedConfigMap)
            case .secret:
                genericResourceDetails(resource: viewModel.state.selectedSecret)
            default:
                Text("Select a config resource")
                    .foregroundStyle(.secondary)
            }
        }
    }

    private var storageDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .node:
                genericResourceDetails(resource: viewModel.state.selectedNode)
            default:
                Text("Select a node")
                    .foregroundStyle(.secondary)
            }
        }
    }

    private var podDetails: some View {
        Group {
            if let pod = viewModel.state.selectedPod {
                VStack(alignment: .leading, spacing: 12) {
                    Text(pod.name)
                        .font(.title2.weight(.bold))

                    Picker("Inspector", selection: $podInspectorTab) {
                        ForEach(PodInspectorTab.allCases) { tab in
                            Text(tab.title).tag(tab)
                        }
                    }
                    .pickerStyle(.segmented)

                    switch podInspectorTab {
                    case .overview:
                        VStack(alignment: .leading, spacing: 10) {
                            Label("Namespace: \(pod.namespace)", systemImage: "square.stack.3d.up")
                            Label("Status: \(pod.status)", systemImage: "waveform.path.ecg")
                            HStack {
                                Button("Delete") { viewModel.requestDeleteSelectedResource() }
                                    .disabled(!viewModel.writeActionsEnabled)
                                Button("Apply YAML") { viewModel.requestApplySelectedResourceYAML() }
                                    .disabled(!viewModel.writeActionsEnabled)
                                Button("Save YAML") { viewModel.saveCurrentResourceYAML() }
                            }
                        }

                    case .logs:
                        VStack(alignment: .leading, spacing: 10) {
                            logsToolbar
                            ScrollView {
                                Text(viewModel.state.podLogs.isEmpty ? "No logs loaded" : viewModel.state.podLogs)
                                    .font(.system(size: 12, weight: .regular, design: .monospaced))
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .textSelection(.enabled)
                                    .padding(10)
                                    .background(editorFill, in: RoundedRectangle(cornerRadius: 8, style: .continuous))
                            }
                        }

                    case .exec:
                        execPane(for: pod)

                    case .portForward:
                        portForwardPane(targetKind: .pod, targetName: pod.name)

                    case .yaml:
                        yamlBlock
                    }
                }
            } else {
                Text("Select a pod")
                    .foregroundStyle(.secondary)
            }
        }
    }

    private var deploymentDetails: some View {
        Group {
            if let deployment = viewModel.state.selectedDeployment {
                VStack(alignment: .leading, spacing: 12) {
                    Text(deployment.name)
                        .font(.title2.weight(.bold))

                    Picker("Inspector", selection: $deploymentInspectorTab) {
                        ForEach(DeploymentInspectorTab.allCases) { tab in
                            Text(tab.title).tag(tab)
                        }
                    }
                    .pickerStyle(.segmented)

                    switch deploymentInspectorTab {
                    case .overview:
                        VStack(alignment: .leading, spacing: 10) {
                            Label("Namespace: \(deployment.namespace)", systemImage: "square.stack.3d.up")
                            Label("Replicas: \(deployment.replicaText)", systemImage: "gauge.with.needle")

                            HStack(spacing: 12) {
                                Stepper(value: $viewModel.scaleReplicaInput, in: 0...500) {
                                    Text("Scale to: \(viewModel.scaleReplicaInput)")
                                }
                                .frame(maxWidth: 180)

                                Button("Scale") { viewModel.requestScaleSelectedDeployment() }
                                    .disabled(!viewModel.writeActionsEnabled)
                                Button("Restart Rollout") { viewModel.requestRolloutRestartSelectedDeployment() }
                                    .disabled(!viewModel.writeActionsEnabled)
                                Button("Delete") { viewModel.requestDeleteSelectedResource() }
                                    .disabled(!viewModel.writeActionsEnabled)
                                Button("Apply YAML") { viewModel.requestApplySelectedResourceYAML() }
                                    .disabled(!viewModel.writeActionsEnabled)
                            }
                        }

                    case .unifiedLogs:
                        VStack(alignment: .leading, spacing: 10) {
                            logsToolbar

                            if !viewModel.state.unifiedServiceLogPods.isEmpty {
                                Text("Pods: " + viewModel.state.unifiedServiceLogPods.joined(separator: ", "))
                                    .font(.footnote.weight(.medium))
                                    .foregroundStyle(.secondary)
                            }

                            ScrollView {
                                Text(viewModel.state.unifiedServiceLogs.isEmpty ? "No unified logs loaded" : viewModel.state.unifiedServiceLogs)
                                    .font(.system(size: 12, weight: .regular, design: .monospaced))
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .textSelection(.enabled)
                                    .padding(10)
                                    .background(editorFill, in: RoundedRectangle(cornerRadius: 8, style: .continuous))
                            }
                        }

                    case .yaml:
                        yamlBlock
                    }
                }
            } else {
                Text("Select a deployment")
                    .foregroundStyle(.secondary)
            }
        }
    }

    private var serviceDetails: some View {
        Group {
            if let service = viewModel.state.selectedService {
                VStack(alignment: .leading, spacing: 12) {
                    Text(service.name)
                        .font(.title2.weight(.bold))

                    Picker("Inspector", selection: $serviceInspectorTab) {
                        ForEach(ServiceInspectorTab.allCases) { tab in
                            Text(tab.title).tag(tab)
                        }
                    }
                    .pickerStyle(.segmented)

                    switch serviceInspectorTab {
                    case .overview:
                        VStack(alignment: .leading, spacing: 10) {
                            Label("Namespace: \(service.namespace)", systemImage: "square.stack.3d.up")
                            Label("Type: \(service.type)", systemImage: "point.3.connected.trianglepath.dotted")
                            Label("Cluster IP: \(service.clusterIP)", systemImage: "network")
                            HStack {
                                Button("Delete") { viewModel.requestDeleteSelectedResource() }
                                    .disabled(!viewModel.writeActionsEnabled)
                                Button("Apply YAML") { viewModel.requestApplySelectedResourceYAML() }
                                    .disabled(!viewModel.writeActionsEnabled)
                                Button("Save YAML") { viewModel.saveCurrentResourceYAML() }
                            }
                        }

                    case .unifiedLogs:
                        VStack(alignment: .leading, spacing: 10) {
                            logsToolbar

                            if !viewModel.state.unifiedServiceLogPods.isEmpty {
                                Text("Pods: " + viewModel.state.unifiedServiceLogPods.joined(separator: ", "))
                                    .font(.footnote.weight(.medium))
                                    .foregroundStyle(.secondary)
                            }

                            ScrollView {
                                Text(viewModel.state.unifiedServiceLogs.isEmpty ? "No unified logs loaded" : viewModel.state.unifiedServiceLogs)
                                    .font(.system(size: 12, weight: .regular, design: .monospaced))
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .textSelection(.enabled)
                                    .padding(10)
                                    .background(editorFill, in: RoundedRectangle(cornerRadius: 8, style: .continuous))
                            }
                        }

                    case .portForward:
                        portForwardPane(targetKind: .service, targetName: service.name)

                    case .yaml:
                        yamlBlock
                    }
                }
            } else {
                Text("Select a service")
                    .foregroundStyle(.secondary)
            }
        }
    }

    private var eventDetails: some View {
        Group {
            if let event = viewModel.state.selectedEvent {
                VStack(alignment: .leading, spacing: 8) {
                    Text(event.reason)
                        .font(.title2.weight(.bold))
                    Text("Type: \(event.type)")
                    Text("Object: \(event.objectName)")
                    ScrollView {
                        Text(event.message)
                            .font(.system(size: 12, weight: .regular, design: .monospaced))
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .padding(10)
                            .background(editorFill, in: RoundedRectangle(cornerRadius: 8, style: .continuous))
                    }
                }
            } else {
                Text("Select an event")
                    .foregroundStyle(.secondary)
            }
        }
    }

    private func genericResourceDetails(resource: ClusterResourceSummary?) -> some View {
        Group {
            if let resource {
                VStack(alignment: .leading, spacing: 12) {
                    Text(resource.name)
                        .font(.title2.weight(.bold))
                        .help(resource.name)

                    VStack(alignment: .leading, spacing: 8) {
                        if let namespace = resource.namespace {
                            Label("Namespace: \(namespace)", systemImage: "square.stack.3d.up")
                        }
                        Label(resource.primaryText, systemImage: "info.circle")
                        Label(resource.secondaryText, systemImage: "text.alignleft")
                    }
                    .font(.subheadline)

                    HStack {
                        Button("Apply YAML") {
                            viewModel.requestApplySelectedResourceYAML()
                        }
                        .disabled(!viewModel.writeActionsEnabled)

                        Button("Save YAML") {
                            viewModel.saveCurrentResourceYAML()
                        }

                        Spacer()
                    }
                    .buttonStyle(.bordered)

                    yamlBlock
                }
            } else {
                Text("Select a resource")
                    .foregroundStyle(.secondary)
            }
        }
    }

    private var yamlBlock: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Button("Apply YAML") {
                    viewModel.requestApplySelectedResourceYAML()
                }
                .disabled(!viewModel.writeActionsEnabled)
                Button("Save YAML") {
                    viewModel.saveCurrentResourceYAML()
                }
                Spacer()
            }

            ScrollView {
                Text(viewModel.state.resourceYAML.isEmpty ? "No YAML loaded" : viewModel.state.resourceYAML)
                    .font(.system(size: 12, weight: .regular, design: .monospaced))
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .textSelection(.enabled)
                    .padding(10)
                    .background(editorFill, in: RoundedRectangle(cornerRadius: 8, style: .continuous))
            }
        }
    }

    private var logsToolbar: some View {
        HStack {
            Picker("Since", selection: $viewModel.selectedLogPreset) {
                ForEach(PodLogPreset.allCases) { preset in
                    Text(preset.title).tag(preset)
                }
            }
            .frame(maxWidth: 190)

            Toggle("Previous", isOn: $viewModel.includePreviousLogs)

            Spacer()

            Button("Reload") {
                viewModel.reloadLogsForSelection()
            }

            Button("Save Logs") {
                viewModel.saveCurrentLogs()
            }
        }
    }

    private func execPane(for pod: PodSummary) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            TextField("Command", text: $viewModel.execCommandInput)
                .textFieldStyle(.roundedBorder)

            HStack(spacing: 10) {
                Button("Run in Pod") {
                    viewModel.requestExecInSelectedPod()
                }
                .disabled(viewModel.state.isExecutingCommand || !viewModel.writeActionsEnabled)

                if viewModel.state.isExecutingCommand {
                    ProgressView()
                        .controlSize(.small)
                }

                Spacer()

                if let result = viewModel.state.lastExecResult, result.podName == pod.name {
                    Text("Exit code: \(result.exitCode)")
                        .font(.footnote.weight(.semibold))
                        .foregroundStyle(result.exitCode == 0 ? Color.secondary : Color.red)
                }
            }

            if let result = viewModel.state.lastExecResult, result.podName == pod.name {
                VStack(alignment: .leading, spacing: 8) {
                    Text(result.command.joined(separator: " "))
                        .font(.footnote.weight(.semibold))
                        .foregroundStyle(.secondary)

                    ScrollView {
                        Text(execOutputText(for: result))
                            .font(.system(size: 12, weight: .regular, design: .monospaced))
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .textSelection(.enabled)
                            .padding(10)
                            .background(editorFill, in: RoundedRectangle(cornerRadius: 8, style: .continuous))
                    }
                }
            } else {
                Text("Run a command to see stdout/stderr here.")
                    .foregroundStyle(.secondary)
            }
        }
    }

    private func portForwardPane(targetKind: PortForwardTargetKind, targetName: String) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(spacing: 10) {
                TextField("Local Port", text: $viewModel.portForwardLocalPortInput)
                    .textFieldStyle(.roundedBorder)
                    .frame(maxWidth: 120)

                Text("->")
                    .foregroundStyle(.secondary)

                TextField("Remote Port", text: $viewModel.portForwardRemotePortInput)
                    .textFieldStyle(.roundedBorder)
                    .frame(maxWidth: 120)

                TextField("Address", text: $viewModel.portForwardAddressInput)
                    .textFieldStyle(.roundedBorder)
                    .frame(maxWidth: 180)
            }

            HStack(spacing: 10) {
                Button("Start Port Forward") {
                    viewModel.startPortForwardForSelection()
                }
                .disabled(viewModel.state.isStartingPortForward)

                if viewModel.state.isStartingPortForward {
                    ProgressView()
                        .controlSize(.small)
                }

                Spacer()

                Button("Open Terminal") {
                    viewModel.setSection(.terminal)
                }
            }

            let matchingSessions = viewModel.state.portForwardSessions.filter {
                $0.targetKind == targetKind && $0.targetName == targetName
            }

            if matchingSessions.isEmpty {
                Text("No active or recent port-forward sessions for this \(targetKind.title.lowercased()).")
                    .foregroundStyle(.secondary)
            } else {
                ForEach(matchingSessions) { session in
                    portForwardSessionRow(session)
                }
            }
        }
    }

    private var terminalPane: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 14) {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Active Port Forwards")
                        .font(.headline)

                    if viewModel.state.portForwardSessions.isEmpty {
                        Text("No port-forward sessions started yet.")
                            .foregroundStyle(.secondary)
                    } else {
                        ForEach(viewModel.state.portForwardSessions) { session in
                            portForwardSessionRow(session)
                        }
                    }
                }
                .padding(12)
                .background(panelFill, in: RoundedRectangle(cornerRadius: 12, style: .continuous))

                VStack(alignment: .leading, spacing: 8) {
                    Text("Last Exec Result")
                        .font(.headline)

                    if let result = viewModel.state.lastExecResult {
                        Text("\(result.podName) • \(result.command.joined(separator: " "))")
                            .font(.footnote.weight(.semibold))
                            .foregroundStyle(.secondary)

                        ScrollView {
                            Text(execOutputText(for: result))
                                .font(.system(size: 12, weight: .regular, design: .monospaced))
                                .frame(maxWidth: .infinity, alignment: .leading)
                                .textSelection(.enabled)
                                .padding(10)
                                .background(editorFill, in: RoundedRectangle(cornerRadius: 8, style: .continuous))
                        }
                        .frame(minHeight: 180)
                    } else {
                        Text("No exec command has been run yet.")
                            .foregroundStyle(.secondary)
                    }
                }
                .padding(12)
                .background(panelFill, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
            }
        }
    }

    private var terminalDetails: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Terminal")
                .font(.title2.weight(.bold))

            Text("Use pod exec for one-shot commands and port-forward sessions for local tunneling.")
                .foregroundStyle(.secondary)

            if let result = viewModel.state.lastExecResult {
                Label("Last exec: \(result.podName)", systemImage: "terminal")
                    .font(.subheadline.weight(.medium))
            }

            if let active = viewModel.state.portForwardSessions.first(where: { $0.status == .active || $0.status == .starting }) {
                Label("\(active.resourceLabel) \(active.localPort):\(active.remotePort)", systemImage: "point.3.connected.trianglepath.dotted")
                    .font(.subheadline.weight(.medium))
            }

            Spacer()
        }
    }

    private func portForwardSessionRow(_ session: PortForwardSession) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text("\(session.resourceLabel)  \(session.localPort):\(session.remotePort)")
                    .font(.subheadline.weight(.semibold))
                Spacer()
                Text(session.status.rawValue.capitalized)
                    .font(.caption.weight(.semibold))
                    .padding(.horizontal, 8)
                    .padding(.vertical, 3)
                    .background(portForwardStatusColor(session.status).opacity(0.16), in: Capsule())
                    .foregroundStyle(portForwardStatusColor(session.status))
            }

            Text("\(session.contextName) • \(session.namespace) • \(session.address)")
                .font(.caption)
                .foregroundStyle(.secondary)

            if !session.lastMessage.isEmpty {
                Text(session.lastMessage)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)
            }

            if session.status == .starting || session.status == .active {
                Button("Stop") {
                    viewModel.stopPortForward(session)
                }
            }
        }
        .padding(10)
        .background(panelFill, in: RoundedRectangle(cornerRadius: 10, style: .continuous))
    }

    private func genericResourceList(
        _ resources: [ClusterResourceSummary],
        selection: ClusterResourceSummary?,
        action: @escaping (ClusterResourceSummary?) -> Void
    ) -> some View {
        List(resources) { resource in
            Button {
                action(resource)
            } label: {
                HStack(alignment: .top, spacing: 12) {
                    VStack(alignment: .leading, spacing: 4) {
                        Text(resource.name)
                            .font(.body.weight(.medium))
                            .lineLimit(1)
                            .help(resource.name)
                        Text(resource.primaryText)
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(.secondary)
                        Text(resource.secondaryText)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                    }
                    Spacer()
                    if let namespace = resource.namespace {
                        Text(namespace)
                            .font(.caption2.weight(.semibold))
                            .padding(.horizontal, 6)
                            .padding(.vertical, 3)
                            .background(Color.secondary.opacity(0.12), in: RoundedRectangle(cornerRadius: 6, style: .continuous))
                            .foregroundStyle(.secondary)
                    }
                }
                .padding(.vertical, 4)
                .background(
                    RoundedRectangle(cornerRadius: 6, style: .continuous)
                        .fill(selection == resource ? Color.accentColor.opacity(0.12) : Color.clear)
                )
            }
            .buttonStyle(.plain)
        }
    }

    private var productionBanner: some View {
        HStack(spacing: 8) {
            Image(systemName: "exclamationmark.triangle.fill")
            Text("Production context active")
                .font(.subheadline.weight(.bold))
        }
        .padding(.horizontal, 14)
        .padding(.vertical, 7)
        .background(Color.red.opacity(0.87), in: Capsule())
    }

    private var commandPalettePresentedBinding: Binding<Bool> {
        Binding(
            get: { viewModel.state.isCommandPalettePresented },
            set: { value in
                if value {
                    viewModel.presentCommandPalette()
                } else {
                    viewModel.dismissCommandPalette()
                }
            }
        )
    }

    private var pendingWriteActionPresentedBinding: Binding<Bool> {
        Binding(
            get: { viewModel.pendingWriteAction != nil },
            set: { value in
                if !value {
                    viewModel.cancelPendingWriteAction()
                }
            }
        )
    }

    private var overviewStatusBanner: some View {
        HStack(spacing: 8) {
            Image(systemName: viewModel.isProductionContext ? "exclamationmark.shield.fill" : "checkmark.shield.fill")
                .foregroundStyle(viewModel.isProductionContext ? .red : .green)
            Text(viewModel.isProductionContext ? "Production context active" : "Non-production context")
                .font(.subheadline.weight(.semibold))
            Spacer()
            Text(viewModel.state.isReadOnlyMode ? "Read-only" : "Read/Write")
                .font(.caption.weight(.bold))
                .padding(.horizontal, 8)
                .frame(height: 28)
                .background((viewModel.state.isReadOnlyMode ? Color.orange : Color.green).opacity(0.24), in: RoundedRectangle(cornerRadius: 8, style: .continuous))
        }
        .padding(12)
        .background(panelFill, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
    }

    private func overviewStatCard(title: String, value: String, symbol: String, tint: Color) -> some View {
        VStack(alignment: .leading, spacing: 7) {
            HStack {
                Image(systemName: symbol)
                    .foregroundStyle(tint)
                Spacer()
            }
            Text(value)
                .font(.title2.weight(.bold))
            Text(title)
                .font(.caption.weight(.medium))
                .foregroundStyle(.secondary)
        }
        .padding(12)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(panelFill, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
    }

    private func healthBadge(label: String, value: Int, color: Color) -> some View {
        HStack(spacing: 6) {
            Circle()
                .fill(color)
                .frame(width: 7, height: 7)
            Text(label + ": \(value)")
                .font(.caption.weight(.semibold))
        }
        .padding(.horizontal, 9)
        .padding(.vertical, 5)
        .background(color.opacity(0.14), in: Capsule())
    }

    private var namespaceSuggestions: [String] {
        Array(Set([viewModel.state.selectedNamespace, "default", "kube-system", "kube-public", "prod"]))
            .filter { !$0.isEmpty }
            .sorted()
    }

    private var panelFill: some ShapeStyle {
        .regularMaterial
    }

    private var editorFill: Color {
        Color(nsColor: .textBackgroundColor)
    }

    private func podStatusCount(_ status: String) -> Int {
        viewModel.state.pods.filter { $0.status.lowercased() == status }.count
    }

    private func statusColor(for status: String) -> Color {
        switch status.lowercased() {
        case "running": return .green
        case "pending": return .orange
        case "failed": return .red
        case "succeeded": return .blue
        default: return .gray
        }
    }

    private func portForwardStatusColor(_ status: PortForwardStatus) -> Color {
        switch status {
        case .starting: return .orange
        case .active: return .green
        case .stopped: return .secondary
        case .failed: return .red
        }
    }

    private func execOutputText(for result: PodExecResult) -> String {
        let stdout = result.stdout.isEmpty ? "" : result.stdout
        let stderr = result.stderr.isEmpty ? "" : "\n[stderr]\n\(result.stderr)"
        let merged = stdout + stderr
        return merged.isEmpty ? "No output" : merged
    }
}
