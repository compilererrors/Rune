import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

@MainActor
final class CommandPaletteViewTests: XCTestCase {
    func testCommonPrefixShortcutsStaySmallAndProduceFocusedQueries() {
        let shortcuts = CommandPalettePresentation.commonPrefixShortcuts

        XCTAssertEqual(shortcuts.count, 4)
        XCTAssertEqual(shortcuts.map(\.queryPrefix), [":po", ":deploy", ":svc", ":ns"])
        XCTAssertEqual(
            shortcuts.map { CommandPalettePresentation.prefillQuery(for: $0.queryPrefix) },
            [":po ", ":deploy ", ":svc ", ":ns "]
        )
        XCTAssertEqual(Set(shortcuts.map(\.id)).count, shortcuts.count)
    }

    func testCheatSheetTitlesResolveToTheFirstActionablePrefix() {
        XCTAssertEqual(
            CommandPalettePresentation.prefillQuery(fromCheatSheetTitle: ":po <name>"),
            ":po "
        )
        XCTAssertEqual(
            CommandPalettePresentation.prefillQuery(fromCheatSheetTitle: ":svc / :service <name>"),
            ":svc "
        )
        XCTAssertEqual(
            CommandPalettePresentation.prefillQuery(fromCheatSheetTitle: "  :ctx <context>  "),
            ":ctx "
        )
        XCTAssertNil(CommandPalettePresentation.prefillQuery(fromCheatSheetTitle: ":"))
        XCTAssertNil(CommandPalettePresentation.prefillQuery(fromCheatSheetTitle: "Pods"))
    }

    func testPaletteGeometryStaysBoundedAtDefaultAndEnlargedTextSizes() {
        let viewModel = RuneAppViewModel(state: RuneAppState())
        let defaultHost = NSHostingView(rootView: AnyView(
            CommandPaletteView(viewModel: viewModel)
        ))
        let enlargedHost = NSHostingView(rootView: AnyView(
            CommandPaletteView(viewModel: viewModel)
                .dynamicTypeSize(.accessibility3)
        ))

        for host in [defaultHost, enlargedHost] {
            let size = host.fittingSize
            XCTAssertGreaterThanOrEqual(size.width, RuneUILayoutMetrics.commandPaletteMinWidth)
            XCTAssertLessThanOrEqual(size.width, RuneUILayoutMetrics.commandPaletteMaxWidth)
            XCTAssertGreaterThanOrEqual(size.height, RuneUILayoutMetrics.commandPaletteMinHeight)
            XCTAssertLessThanOrEqual(size.height, RuneUILayoutMetrics.commandPaletteMaxHeight)
        }
    }

    func testActivationGateExecutesOnlyOnceAfterPaletteDismissal() {
        let gate = CommandPaletteActivationGate()
        var isPresented = true
        var executionCount = 0

        for _ in 0..<64 {
            gate.perform(isPalettePresented: { isPresented }) {
                executionCount += 1
                isPresented = false
            }
        }

        XCTAssertEqual(executionCount, 1)
        XCTAssertTrue(gate.isExecuting)
    }

    func testActivationGateUnlocksAfterRejectedCommandLeavesPaletteOpen() {
        let gate = CommandPaletteActivationGate()
        var executionCount = 0

        for _ in 0..<2 {
            gate.perform(isPalettePresented: { true }) {
                executionCount += 1
            }
        }

        XCTAssertEqual(executionCount, 2)
        XCTAssertFalse(gate.isExecuting)
    }

    func testStaleResourceItemResolvesNewestModelByStableIDBeforeNavigation() throws {
        let state = RuneAppState()
        let stalePod = PodSummary(
            name: "synthetic-api-0",
            namespace: "default",
            status: "Pending"
        )
        state.setPods([stalePod])
        let viewModel = RuneAppViewModel(state: state)
        let item = try XCTUnwrap(
            viewModel.commandPaletteItems(query: stalePod.name).first { item in
                if case .pod = item.action { return true }
                return false
            }
        )
        let refreshedPod = PodSummary(
            name: stalePod.name,
            namespace: stalePod.namespace,
            status: "Running",
            totalRestarts: 3
        )
        state.setPods([refreshedPod])
        state.setSelectedPod(nil)

        viewModel.executeCommandPaletteItem(item)

        XCTAssertEqual(state.selectedPod, refreshedPod)
        XCTAssertEqual(state.selectedPod?.status, "Running")
        XCTAssertEqual(state.selectedPod?.totalRestarts, 3)
    }

    func testEveryResourceActionFamilyResolvesNewestModelByStableID() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)

        let staleDeployment = DeploymentSummary(
            name: "synthetic-deployment",
            namespace: "default",
            readyReplicas: 0,
            desiredReplicas: 2
        )
        let currentDeployment = DeploymentSummary(
            name: staleDeployment.name,
            namespace: staleDeployment.namespace,
            readyReplicas: 2,
            desiredReplicas: 2
        )
        state.setDeployments([currentDeployment])
        state.setSelectedDeployment(nil)

        viewModel.executeCommandPaletteItem(paletteItem(action: .deployment(staleDeployment)))

        XCTAssertEqual(state.selectedDeployment, currentDeployment)
        XCTAssertEqual(state.selectedDeployment?.readyReplicas, 2)

        let staleService = ServiceSummary(
            name: "synthetic-service",
            namespace: "default",
            type: "ClusterIP",
            clusterIP: "None"
        )
        let currentService = ServiceSummary(
            name: staleService.name,
            namespace: staleService.namespace,
            type: "LoadBalancer",
            clusterIP: "192.0.2.10"
        )
        state.setServices([currentService])
        state.setSelectedService(nil)

        viewModel.executeCommandPaletteItem(paletteItem(action: .service(staleService)))

        XCTAssertEqual(state.selectedService, currentService)
        XCTAssertEqual(state.selectedService?.type, "LoadBalancer")

        let staleEvent = EventSummary(
            eventIdentifier: "synthetic-event-id",
            eventNamespace: "default",
            type: "Normal",
            reason: "Scheduled",
            objectName: "synthetic-pod",
            message: "old"
        )
        let currentEvent = EventSummary(
            eventIdentifier: staleEvent.eventIdentifier,
            eventNamespace: staleEvent.eventNamespace,
            type: "Warning",
            reason: "BackOff",
            objectName: staleEvent.objectName,
            message: "current"
        )
        state.setEvents([currentEvent])
        state.setSelectedEvent(nil)

        viewModel.executeCommandPaletteItem(paletteItem(action: .event(staleEvent)))

        XCTAssertEqual(state.selectedEvent, currentEvent)
        XCTAssertEqual(state.selectedEvent?.message, "current")

        let staleRelease = HelmReleaseSummary(
            name: "synthetic-release",
            namespace: "default",
            revision: 1,
            updated: "old",
            status: "pending",
            chart: "synthetic-chart-1",
            appVersion: "1"
        )
        let currentRelease = HelmReleaseSummary(
            name: staleRelease.name,
            namespace: staleRelease.namespace,
            revision: 2,
            updated: "current",
            status: "deployed",
            chart: "synthetic-chart-2",
            appVersion: "2"
        )
        state.setHelmReleases([currentRelease])
        state.setSelectedHelmRelease(nil)

        viewModel.executeCommandPaletteItem(paletteItem(action: .helmRelease(staleRelease)))

        XCTAssertEqual(state.selectedHelmRelease, currentRelease)
        XCTAssertEqual(state.selectedHelmRelease?.revision, 2)

        let staleConfigMap = ClusterResourceSummary(
            kind: .configMap,
            name: "synthetic-config",
            namespace: "default",
            primaryText: "old",
            secondaryText: ""
        )
        let currentConfigMap = ClusterResourceSummary(
            kind: staleConfigMap.kind,
            name: staleConfigMap.name,
            namespace: staleConfigMap.namespace,
            primaryText: "current",
            secondaryText: ""
        )
        state.setConfigMaps([currentConfigMap])
        state.setSelectedConfigMap(nil)

        viewModel.executeCommandPaletteItem(paletteItem(action: .clusterResource(staleConfigMap)))

        XCTAssertEqual(state.selectedConfigMap, currentConfigMap)
        XCTAssertEqual(state.selectedConfigMap?.primaryText, "current")
    }

    func testRemovedResourceItemCannotNavigateToCapturedStaleObject() throws {
        let state = RuneAppState()
        let removedPod = PodSummary(
            name: "synthetic-removed-0",
            namespace: "default",
            status: "Running"
        )
        let remainingPod = PodSummary(
            name: "synthetic-current-0",
            namespace: "default",
            status: "Running"
        )
        state.setPods([removedPod])
        let viewModel = RuneAppViewModel(state: state)
        let item = try XCTUnwrap(
            viewModel.commandPaletteItems(query: removedPod.name).first { item in
                if case .pod = item.action { return true }
                return false
            }
        )
        state.setPods([remainingPod])
        viewModel.presentCommandPalette()

        viewModel.executeCommandPaletteItem(item)

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertEqual(state.selectedPod, remainingPod)
        XCTAssertFalse(state.isCommandPalettePresented)
    }

    func testRemovedClusterResourceItemCannotNavigateToCapturedStaleObject() {
        let state = RuneAppState()
        let removedConfigMap = ClusterResourceSummary(
            kind: .configMap,
            name: "synthetic-removed",
            namespace: "default",
            primaryText: "removed",
            secondaryText: ""
        )
        let currentConfigMap = ClusterResourceSummary(
            kind: .configMap,
            name: "synthetic-current",
            namespace: "default",
            primaryText: "current",
            secondaryText: ""
        )
        state.setConfigMaps([currentConfigMap])
        state.selectedSection = .overview
        let viewModel = RuneAppViewModel(state: state)
        viewModel.presentCommandPalette()

        viewModel.executeCommandPaletteItem(
            paletteItem(action: .clusterResource(removedConfigMap))
        )

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertEqual(state.selectedConfigMap, currentConfigMap)
        XCTAssertFalse(state.isCommandPalettePresented)
    }

    func testRemovedNamespaceItemCannotChangeCurrentNamespace() throws {
        let state = RuneAppState()
        state.setContexts([KubeContext(name: "synthetic-context")])
        state.setNamespaces(["synthetic-current", "synthetic-removed"])
        let viewModel = RuneAppViewModel(state: state)
        let item = try XCTUnwrap(
            viewModel.commandPaletteItems(query: ":ns synthetic-removed").first { item in
                if case .namespace("synthetic-removed") = item.action { return true }
                return false
            }
        )
        state.setNamespaces(["synthetic-current"])
        state.selectedNamespace = "synthetic-current"
        viewModel.presentCommandPalette()

        viewModel.executeCommandPaletteItem(item)

        XCTAssertEqual(state.selectedNamespace, "synthetic-current")
        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertFalse(state.isCommandPalettePresented)
    }

    func testPaletteSourceUsesOneInstructionActionablePrefixesAndFullHelp() throws {
        let source = try String(contentsOfFile: commandPaletteViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("Search by name, or type : to browse command prefixes."))
        XCTAssertTrue(source.contains("TextField(\"Search commands and resources\""))
        XCTAssertFalse(source.contains("Search or use a prefix:"))
        XCTAssertFalse(source.contains("Search or type e.g."))
        XCTAssertFalse(source.contains("private func paletteHint"))
        XCTAssertTrue(source.contains("ForEach(CommandPalettePresentation.commonPrefixShortcuts)"))
        XCTAssertTrue(source.contains("applyPrefix(shortcut.queryPrefix)"))
        XCTAssertTrue(source.contains("Label(\"All Prefixes\", systemImage: \"questionmark.circle\")"))
        XCTAssertTrue(source.contains("let prefixItems = viewModel.commandPaletteItems(query: \":\")"))
        XCTAssertTrue(source.contains("ForEach(prefixItems)"))
        XCTAssertTrue(source.contains("CommandPalettePresentation.prefillQuery("))
        XCTAssertTrue(source.contains("fromCheatSheetTitle: item.title"))
        XCTAssertTrue(source.contains("title: \"No Commands Found\""))
        XCTAssertTrue(source.contains("private func executePrimaryAction(items: [CommandPaletteItem])"))
        XCTAssertTrue(source.contains("private func handleLocalKeyEvent(_ event: NSEvent, items: [CommandPaletteItem])"))
        XCTAssertTrue(source.contains("guard !isPrefixHelpPresented else { return false }"))
        XCTAssertTrue(source.contains("if !isPrefixHelpPresented {\n            VStack(spacing: 0)"))
        XCTAssertTrue(source.contains(".focused($prefixHelpFocusedItemID, equals: item.id)"))
    }

    private var commandPaletteViewPath: String {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/CommandPaletteView.swift")
            .path
    }

    private func paletteItem(action: CommandPaletteItem.Action) -> CommandPaletteItem {
        CommandPaletteItem(
            id: "synthetic-command",
            title: "Synthetic",
            subtitle: "Synthetic command",
            symbolName: "circle",
            action: action
        )
    }
}
