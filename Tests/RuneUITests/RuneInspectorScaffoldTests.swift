import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class RuneInspectorScaffoldTests: XCTestCase {
    @MainActor
    func testVerticalBodyAddsOneVerticalViewportAndFitsMinimumInspectorWidth() {
        let host = hostingView(scrollBehavior: .vertical, width: 320, height: 584)

        XCTAssertEqual(host.frame.width, RuneInspectorScaffoldMetrics.supportedMinimumWidth)
        XCTAssertGreaterThanOrEqual(scrollViews(in: host).count, 2, "Body and action row should own distinct scroll axes.")
        XCTAssertLessThanOrEqual(host.fittingSize.width, 321)
        XCTAssertEqual(RuneInspectorScaffoldMetrics.sectionSpacing, 12)
    }

    @MainActor
    func testSelfManagedBodyDoesNotIntroduceAnExtraVerticalViewport() {
        let plainHost = hostingView(scrollBehavior: .selfManaged, width: 320, height: 584)
        let nestedHost = NSHostingView(rootView: scaffold(
            scrollBehavior: .selfManaged,
            content: {
                ScrollView(.vertical) {
                    Text(String(repeating: "self managed\n", count: 40))
                }
            }
        ).frame(width: 320, height: 420))
        nestedHost.frame = NSRect(x: 0, y: 0, width: 320, height: 420)
        nestedHost.layoutSubtreeIfNeeded()

        let plainScrollCount = scrollViews(in: plainHost).count
        let nestedScrollCount = scrollViews(in: nestedHost).count
        XCTAssertEqual(nestedScrollCount, plainScrollCount + 1)
    }

    @MainActor
    func testRenderedCompactInspectorRegionsArePositiveOrderedAndNonOverlapping() throws {
        var latestSnapshot: RuneInspectorScaffoldLayoutSnapshot?
        let host = hostingView(
            scrollBehavior: .vertical,
            width: 320,
            height: 584,
            onLayoutSnapshotChange: { latestSnapshot = $0 }
        )
        settle(host, until: { latestSnapshot != nil })

        let snapshot = try XCTUnwrap(latestSnapshot)
        let orderedRegions: [RuneInspectorScaffoldRegion] = [.identity, .info, .tabs, .actions, .body]
        let frames = try orderedRegions.map { region in
            try XCTUnwrap(snapshot[region], "Missing rendered frame for \(region)")
        }

        for frame in frames {
            XCTAssertTrue(frame.minX.isFinite && frame.minY.isFinite)
            XCTAssertTrue(frame.width.isFinite && frame.height.isFinite)
            XCTAssertGreaterThan(frame.width, 0)
            XCTAssertGreaterThan(frame.height, 0)
            XCTAssertGreaterThanOrEqual(frame.minX, -0.5)
            XCTAssertGreaterThanOrEqual(frame.minY, -0.5)
            XCTAssertLessThanOrEqual(frame.maxX, 320.5)
            XCTAssertLessThanOrEqual(frame.maxY, 584.5)
        }

        for (upper, lower) in zip(frames, frames.dropFirst()) {
            XCTAssertLessThanOrEqual(
                upper.maxY,
                lower.minY + 0.5,
                "Inspector regions must keep their documented vertical order without overlap."
            )
        }
        XCTAssertGreaterThan(try XCTUnwrap(snapshot[.body]).height, 200)
    }

    @MainActor
    func testTablessCompactInspectorOmitsTabRegionAndKeepsBodyBelowActions() throws {
        var latestSnapshot: RuneInspectorScaffoldLayoutSnapshot?
        let host = hostingView(
            scrollBehavior: .vertical,
            width: 320,
            height: 584,
            showsTabs: false,
            onLayoutSnapshotChange: { latestSnapshot = $0 }
        )
        settle(host, until: { latestSnapshot != nil })

        let snapshot = try XCTUnwrap(latestSnapshot)
        XCTAssertNil(snapshot[.tabs])
        let identity = try XCTUnwrap(snapshot[.identity])
        let info = try XCTUnwrap(snapshot[.info])
        let actions = try XCTUnwrap(snapshot[.actions])
        let body = try XCTUnwrap(snapshot[.body])
        XCTAssertLessThanOrEqual(identity.maxY, info.minY + 0.5)
        XCTAssertLessThanOrEqual(info.maxY, actions.minY + 0.5)
        XCTAssertLessThanOrEqual(actions.maxY, body.minY + 0.5)
        XCTAssertGreaterThan(body.height, 240)
    }

    @MainActor
    func testCompactInspectorInitialLayoutBenchmarkKPI() {
        let started = ContinuousClock.now
        for _ in 0..<20 {
            autoreleasepool {
                let host = hostingView(scrollBehavior: .vertical, width: 320, height: 584)
                host.layoutSubtreeIfNeeded()
            }
        }
        let elapsed = seconds(started.duration(to: .now))

        #if DEBUG
        let maximumSeconds = 1.2
        #else
        let maximumSeconds = 0.4
        #endif
        XCTAssertLessThan(
            elapsed,
            maximumSeconds,
            "KPI: 20 compact inspector mounts and initial layouts should stay below \(maximumSeconds)s."
        )
    }

    func testScaffoldKeepsIdentityUtilitiesTabsActionsAndScrollPolicyExplicit() throws {
        let source = try String(contentsOfFile: scaffoldPath, encoding: .utf8)

        XCTAssertTrue(source.contains("RuneIconButton("))
        XCTAssertTrue(source.contains("\"Refresh inspector\""))
        XCTAssertTrue(source.contains("RuneInspectorActionRow"))
        XCTAssertTrue(source.contains("case vertical"))
        XCTAssertTrue(source.contains("case selfManaged"))
        XCTAssertTrue(source.contains("ScrollView(.vertical)"))
        XCTAssertTrue(source.contains(".layoutPriority(1)"))
        XCTAssertTrue(source.contains("showsTabs: Bool = true"))
        XCTAssertTrue(source.contains("if showsTabs"))
        XCTAssertTrue(source.contains(".truncationMode(.middle)"))
        XCTAssertTrue(source.contains(".accessibilityElement(children: .contain)"))
    }

    func testGenericInspectorTabRawValuesRemainBackwardCompatible() {
        XCTAssertEqual(GenericResourceManifestTab.allCases, [.overview, .describe, .yaml])
        XCTAssertEqual(GenericResourceManifestTab(rawValue: "overview"), .overview)
        XCTAssertEqual(GenericResourceManifestTab(rawValue: "describe"), .describe)
        XCTAssertEqual(GenericResourceManifestTab(rawValue: "yaml"), .yaml)
        XCTAssertNil(GenericResourceManifestTab(rawValue: "manifest"))
    }

    func testHelmInspectorModeKeepsBrowserSelectionAlignedWithActiveResource() {
        XCTAssertEqual(
            RuneHelmInspectorMode.resolve(hasRelease: false, hasOperatorResource: false),
            .none
        )
        XCTAssertEqual(
            RuneHelmInspectorMode.resolve(hasRelease: true, hasOperatorResource: false).browserTab,
            .releases
        )
        XCTAssertEqual(
            RuneHelmInspectorMode.resolve(hasRelease: false, hasOperatorResource: true).browserTab,
            .operatorResources
        )
        XCTAssertEqual(
            RuneHelmInspectorMode.resolve(hasRelease: true, hasOperatorResource: true),
            .operatorResource,
            "An explicitly selected operator resource must win over a stale release selection during restore."
        )
    }

    func testInspectorRefreshRoutingUsesTheVisibleNetworkingInspectorFamily() {
        let overviewRoute = RuneInspectorRefreshRouting.route(
            section: .networking,
            workloadKind: .ingress,
            podTab: .logs,
            deploymentTab: .unifiedLogs,
            serviceTab: .unifiedLogs,
            genericTab: .overview,
            helmTab: .manifest,
            helmMode: .release
        )
        let yamlRoute = RuneInspectorRefreshRouting.route(
            section: .networking,
            workloadKind: .ingress,
            podTab: .overview,
            deploymentTab: .overview,
            serviceTab: .unifiedLogs,
            genericTab: .yaml,
            helmTab: .overview,
            helmMode: .none
        )

        XCTAssertEqual(overviewRoute, .currentView)
        XCTAssertEqual(yamlRoute, .resourceInspector)
    }

    func testInspectorRefreshRoutingDistinguishesHelmReleaseAndOperatorDocuments() {
        let operatorOverview = RuneInspectorRefreshRouting.route(
            section: .helm,
            workloadKind: .pod,
            podTab: .overview,
            deploymentTab: .overview,
            serviceTab: .overview,
            genericTab: .overview,
            helmTab: .manifest,
            helmMode: .operatorResource
        )
        let operatorYAML = RuneInspectorRefreshRouting.route(
            section: .helm,
            workloadKind: .pod,
            podTab: .overview,
            deploymentTab: .overview,
            serviceTab: .overview,
            genericTab: .yaml,
            helmTab: .overview,
            helmMode: .operatorResource
        )
        let releaseOverview = RuneInspectorRefreshRouting.route(
            section: .helm,
            workloadKind: .pod,
            podTab: .overview,
            deploymentTab: .overview,
            serviceTab: .overview,
            genericTab: .yaml,
            helmTab: .overview,
            helmMode: .release
        )
        let releaseManifest = RuneInspectorRefreshRouting.route(
            section: .helm,
            workloadKind: .pod,
            podTab: .overview,
            deploymentTab: .overview,
            serviceTab: .overview,
            genericTab: .overview,
            helmTab: .manifest,
            helmMode: .release
        )

        XCTAssertEqual(operatorOverview, .currentView)
        XCTAssertEqual(operatorYAML, .helmInspector)
        XCTAssertEqual(releaseOverview, .currentView)
        XCTAssertEqual(releaseManifest, .helmInspector)
    }

    func testManifestDocumentStateKeepsCachedContentVisibleDuringRefreshAndFailure() {
        func resolve(content: String, isLoading: Bool, error: String?) -> ManifestDocumentState {
            ManifestDocumentState.resolved(
                content: content,
                isLoading: isLoading,
                error: error,
                loadingTitle: "Loading",
                loadingMessage: "Fetching",
                failureTitle: "Refresh failed",
                emptyTitle: "Empty",
                emptyMessage: "No content"
            )
        }

        XCTAssertEqual(resolve(content: "cached document", isLoading: true, error: nil), .ready)
        XCTAssertEqual(
            resolve(content: "cached document", isLoading: false, error: "synthetic failure"),
            .stale(title: "Refresh failed", message: "synthetic failure")
        )
        XCTAssertEqual(
            resolve(content: "", isLoading: true, error: nil),
            .loading(title: "Loading", message: "Fetching")
        )
        XCTAssertEqual(
            resolve(content: "", isLoading: false, error: "synthetic failure"),
            .failure(title: "Refresh failed", message: "synthetic failure")
        )
    }

    func testAllResourceInspectorFamiliesUseScaffoldWithExplicitScrollOwnership() throws {
        let source = try RuneRootViewSource.text()
        let pod = try sourceBlock(from: "var podDetails", to: "var deploymentDetails", source: source)
        let deployment = try sourceBlock(from: "var deploymentDetails", to: "var serviceDetails", source: source)
        let service = try sourceBlock(from: "var serviceDetails", to: "func serviceInspectorCoreInfo", source: source)
        let generic = try sourceBlock(
            from: "func genericResourceDetails<Supplementary: View>",
            to: "func genericResourceOverview<Supplementary: View>",
            source: source
        )
        let operatorResource = try sourceBlock(
            from: "func operatorResourceDetails",
            to: "func operatorResourceOverview",
            source: source
        )
        let helm = try sourceBlock(from: "var helmDetails", to: "func helmReleaseCoreInfo", source: source)
        let event = try sourceBlock(from: "var eventDetails", to: "func eventInspectorCoreInfo", source: source)

        for block in [pod, deployment, service] {
            XCTAssertTrue(block.contains("RuneInspectorScaffold("))
            XCTAssertTrue(block.contains("? .vertical : .selfManaged"))
            XCTAssertTrue(block.contains("onRefresh: refreshDetailPane"))
        }
        XCTAssertTrue(pod.contains("showsActions: podInspectorTab == .overview"))
        XCTAssertTrue(deployment.contains("showsActions: deploymentInspectorTab == .overview"))
        XCTAssertTrue(service.contains("showsActions: serviceInspectorTab == .overview"))
        XCTAssertTrue(deployment.contains("RuneAdaptiveToolbar(\"Deployment rollout actions\")"))

        for block in [generic, operatorResource, helm, event] {
            XCTAssertTrue(block.contains("RuneInspectorScaffold("))
            XCTAssertTrue(block.contains("bodyScrollBehavior:"))
            XCTAssertTrue(block.contains("onRefresh: refreshDetailPane"))
        }
        XCTAssertTrue(generic.contains("? .vertical : .selfManaged"))
        XCTAssertTrue(operatorResource.contains("? .vertical : .selfManaged"))
        XCTAssertTrue(helm.contains("? .vertical"))
        XCTAssertTrue(event.contains("showsTabs: false"))
        XCTAssertTrue(event.contains("bodyScrollBehavior: .vertical"))
    }

    func testGenericOverviewOwnsSupplementaryRelationshipsAndRolloutBeforeDocumentTabs() throws {
        let source = try RuneRootViewSource.text()
        let generic = try sourceBlock(
            from: "func genericResourceOverview<Supplementary: View>",
            to: "var yamlManifestDocumentState",
            source: source
        )

        let supplementaryIndex = try XCTUnwrap(generic.range(of: "supplementary()")).lowerBound
        let relationshipIndex = try XCTUnwrap(generic.range(of: "RelatedEventsRelationshipSection")).lowerBound
        let rolloutIndex = try XCTUnwrap(generic.range(of: "rolloutRevisionInput")).lowerBound

        XCTAssertLessThan(supplementaryIndex, relationshipIndex)
        XCTAssertLessThan(relationshipIndex, rolloutIndex)

        for name in [
            "var replicaSetDetails",
            "var statefulSetDetails",
            "var cronJobInspectorContent",
            "var ingressDetails",
            "var persistentVolumeClaimDetails",
            "var rbacDetails"
        ] {
            let start = try XCTUnwrap(source.range(of: name))
            let firstCall = try XCTUnwrap(source.range(of: "genericResourceDetails(resource:", range: start.upperBound..<source.endIndex))
            XCTAssertLessThan(firstCall.lowerBound, source.endIndex)
        }
        XCTAssertTrue(source.contains("case overview"))
        XCTAssertTrue(source.contains("case .overview: return string(.overview)"))
    }

    func testHelmInspectorKeepsActionsTabRelevantAndAvoidsNestedHistoryScroll() throws {
        let source = try RuneRootViewSource.text()
        let helm = try sourceBlock(
            from: "var helmDetails",
            to: "func operatorResourceDetails",
            source: source
        )

        // One rule for facts: identity in the header block (subtitle and
        // status pill), every release fact in one grid on the Overview tab.
        let identityIndex = try XCTUnwrap(helm.range(of: "title: release.name")).lowerBound
        let tabsIndex = try XCTUnwrap(helm.range(of: "RuneTabBar(")).lowerBound
        let overviewIndex = try XCTUnwrap(helm.range(of: "case .overview:\n                            helmReleaseCoreInfo(release)")).lowerBound
        XCTAssertLessThan(identityIndex, tabsIndex)
        XCTAssertLessThan(tabsIndex, overviewIndex)
        XCTAssertTrue(helm.contains("subtitle: inspectorSubtitle(\"Helm release\", namespace: release.namespace)"))
        XCTAssertTrue(helm.contains("status: RuneInspectorStatus(release.status.capitalized"))

        let coreInfo = try sourceBlock(from: "func helmReleaseCoreInfo", to: "var helmInspectorActions", source: source)
        XCTAssertTrue(coreInfo.contains("RuneFactGrid(\"Release\")"))
        XCTAssertFalse(coreInfo.contains("inspectorInfoRow(\"Status\""), "status is the header pill")
        XCTAssertFalse(coreInfo.contains("inspectorInfoRow(\"Namespace\""), "namespace is in the subtitle")
        let chartIndex = try XCTUnwrap(coreInfo.range(of: "inspectorInfoRow(\"Chart\"")).lowerBound
        let updatedIndex = try XCTUnwrap(coreInfo.range(of: "inspectorInfoRow(\"Updated\"")).lowerBound
        XCTAssertLessThan(chartIndex, updatedIndex)
        XCTAssertTrue(helm.contains("var helmInspectorActions"))
        XCTAssertTrue(helm.contains("Save Values…"))
        XCTAssertTrue(helm.contains("Save Manifest…"))
        XCTAssertTrue(helm.contains("Save History…"))

        let history = try sourceBlock(from: "var helmHistoryPane", to: "func operatorResourceDetails", source: source)
        XCTAssertFalse(history.contains("ScrollView"))
        XCTAssertTrue(history.contains("LazyVStack"))
    }

    func testDocumentAndEventBodiesKeepOneVerticalScrollOwner() throws {
        let source = try RuneRootViewSource.text()
        let generic = try sourceBlock(
            from: "func genericResourceDetails<Supplementary: View>",
            to: "func genericResourceOverview<Supplementary: View>",
            source: source
        )
        let operatorResource = try sourceBlock(
            from: "func operatorResourceDetails",
            to: "func operatorResourceOverview",
            source: source
        )
        let event = try sourceBlock(from: "var eventDetails", to: "func eventInspectorCoreInfo", source: source)
        let exportable = try sourceBlock(from: "func exportableTextPane", to: "func execPane", source: source)

        XCTAssertTrue(generic.contains("case .describe, .yaml:"))
        XCTAssertTrue(generic.contains("manifestInspectorPane(activeTab:"))
        XCTAssertTrue(operatorResource.contains("case .describe, .yaml:"))
        XCTAssertFalse(event.contains("ScrollView"))
        XCTAssertTrue(event.contains("showsTabs: false"))
        XCTAssertTrue(exportable.contains("InspectorTextSurface"))
        XCTAssertFalse(exportable.contains("ScrollView"))
    }

    @MainActor
    private func hostingView(
        scrollBehavior: RuneInspectorBodyScrollBehavior,
        width: CGFloat,
        height: CGFloat,
        showsTabs: Bool = true,
        onLayoutSnapshotChange: ((RuneInspectorScaffoldLayoutSnapshot) -> Void)? = nil
    ) -> NSHostingView<some View> {
        let host = NSHostingView(rootView: scaffold(
            scrollBehavior: scrollBehavior,
            showsTabs: showsTabs,
            onLayoutSnapshotChange: onLayoutSnapshotChange,
            content: {
                VStack(alignment: .leading) {
                    ForEach(0..<30, id: \.self) { index in
                        Text("Inspector row \(index)")
                    }
                }
            }
        ).frame(width: width, height: height))
        host.frame = NSRect(x: 0, y: 0, width: width, height: height)
        host.layoutSubtreeIfNeeded()
        return host
    }

    @MainActor
    private func scaffold<Content: View>(
        scrollBehavior: RuneInspectorBodyScrollBehavior,
        showsTabs: Bool = true,
        onLayoutSnapshotChange: ((RuneInspectorScaffoldLayoutSnapshot) -> Void)? = nil,
        @ViewBuilder content: () -> Content
    ) -> some View {
        RuneInspectorScaffold(
            title: "synthetic-resource-with-a-long-name",
            copyAccessibilityLabel: "Copy synthetic resource name",
            bodyScrollBehavior: scrollBehavior,
            showsTabs: showsTabs,
            onCopy: {},
            onRefresh: {},
            onLayoutSnapshotChange: onLayoutSnapshotChange,
            info: {
                RuneInspectorInfoRow("Status") {
                    Text("Ready")
                }
            },
            tabs: {
                Picker("Inspector", selection: .constant(0)) {
                    Text("Overview").tag(0)
                    Text("YAML").tag(1)
                }
                .pickerStyle(.segmented)
            },
            actions: {
                Button("Apply") {}
                Button("Delete") {}
            },
            content: content
        )
    }

    @MainActor
    private func settle<Content: View>(
        _ host: NSHostingView<Content>,
        until condition: () -> Bool
    ) {
        let deadline = Date().addingTimeInterval(1)
        repeat {
            host.layoutSubtreeIfNeeded()
            _ = RunLoop.main.run(mode: .default, before: Date().addingTimeInterval(0.01))
        } while !condition() && Date() < deadline
        host.layoutSubtreeIfNeeded()
    }

    private func seconds(_ duration: Duration) -> Double {
        Double(duration.components.seconds) + Double(duration.components.attoseconds) / 1e18
    }

    @MainActor
    private func scrollViews(in view: NSView) -> [NSScrollView] {
        var result: [NSScrollView] = view is NSScrollView ? [view as! NSScrollView] : []
        for subview in view.subviews {
            result.append(contentsOf: scrollViews(in: subview))
        }
        return result
    }

    private var scaffoldPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Design/Components/RuneInspectorScaffold.swift").path
    }


    private func sourceBlock(from start: String, to end: String, source: String) throws -> String {
        let startRange = try XCTUnwrap(source.range(of: start))
        let endRange = try XCTUnwrap(source.range(of: end, range: startRange.upperBound..<source.endIndex))
        return String(source[startRange.lowerBound..<endRange.lowerBound])
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}
