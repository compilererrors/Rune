import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

/// Layout probes (`RuneRootLayoutSnapshot`) record **minY** and **minX** in window space.
/// Horizontal assertions catch sideways “offset” in the inspector/content when tabs or editors swap.
/// Run the app with `RUNE_DEBUG_LAYOUT=1` to log probe coordinates to the console.
@MainActor
final class RuneRootViewLayoutRegressionTests: XCTestCase {
    /// Vertical probe alignment (top edges).
    private let verticalAlignmentAccuracy: CGFloat = 1.5
    private let detailVerticalAlignmentAccuracy: CGFloat = 3.0
    /// Horizontal probe alignment — catches inspector/content shifting sideways (“offset”) when tabs or editors change.
    private let horizontalOffsetAccuracy: CGFloat = 2.5
    private static let defaultPostSettleObservationDuration: TimeInterval = 0.35
    private static let longPodPostSettleObservationDuration: TimeInterval = 3.5

    func testWorkloadPodDescribeTabRemainsTopAligned() async throws {
        let pod = PodSummary(name: "sample-pod-7c9db", namespace: "team-alpha", status: "Running", totalRestarts: 1, ageDescription: "5m")
        for shellVariant in RuneRootShellVariant.allCases {
            for editorImplementation in ManifestInlineEditorImplementation.allCases {
                let baseline = try await hostSnapshot(
                    viewModel: makePodViewModel(pod: pod),
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialPodInspectorTab: .overview,
                            shellVariant: shellVariant,
                            manifestInlineEditorImplementation: editorImplementation
                        )
                    },
                    section: .workloads,
                    kind: .pod
                )
                let describe = try await hostSnapshot(
                    viewModel: makePodViewModel(pod: pod),
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialPodInspectorTab: .describe,
                            shellVariant: shellVariant,
                            manifestInlineEditorImplementation: editorImplementation
                        )
                    },
                    section: .workloads,
                    kind: .pod
                )

                assertAligned(baseline: baseline, candidate: describe)
            }
        }
    }

    func testWorkloadPodYAMLTabRemainsTopAligned() async throws {
        let pod = PodSummary(name: "sample-pod-7c9db", namespace: "team-alpha", status: "Running", totalRestarts: 1, ageDescription: "5m")
        for shellVariant in RuneRootShellVariant.allCases {
            for editorImplementation in ManifestInlineEditorImplementation.allCases {
                let baseline = try await hostSnapshot(
                    viewModel: makePodViewModel(pod: pod),
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialPodInspectorTab: .overview,
                            shellVariant: shellVariant,
                            manifestInlineEditorImplementation: editorImplementation
                        )
                    },
                    section: .workloads,
                    kind: .pod
                )
                let yaml = try await hostSnapshot(
                    viewModel: makePodViewModel(pod: pod),
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialPodInspectorTab: .yaml,
                            shellVariant: shellVariant,
                            manifestInlineEditorImplementation: editorImplementation,
                            initialYAMLInlineEditing: editorImplementation.supportsInlineEditing
                        )
                    },
                    section: .workloads,
                    kind: .pod
                )

                assertAligned(baseline: baseline, candidate: yaml)
            }
        }
    }

    func testWorkloadDeploymentDescribeTabRemainsTopAligned() async throws {
        let deployment = DeploymentSummary(name: "sample-deployment", namespace: "team-alpha", readyReplicas: 2, desiredReplicas: 2)
        for shellVariant in RuneRootShellVariant.allCases {
            for editorImplementation in ManifestInlineEditorImplementation.allCases {
                let baseline = try await hostSnapshot(
                    viewModel: makeDeploymentViewModel(deployment: deployment),
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialDeploymentInspectorTab: .overview,
                            shellVariant: shellVariant,
                            manifestInlineEditorImplementation: editorImplementation
                        )
                    },
                    section: .workloads,
                    kind: .deployment
                )
                let describe = try await hostSnapshot(
                    viewModel: makeDeploymentViewModel(deployment: deployment),
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialDeploymentInspectorTab: .describe,
                            shellVariant: shellVariant,
                            manifestInlineEditorImplementation: editorImplementation
                        )
                    },
                    section: .workloads,
                    kind: .deployment
                )

                assertAligned(baseline: baseline, candidate: describe)
            }
        }
    }

    func testWorkloadDeploymentYAMLTabRemainsTopAligned() async throws {
        let deployment = DeploymentSummary(name: "sample-deployment", namespace: "team-alpha", readyReplicas: 2, desiredReplicas: 2)
        for shellVariant in RuneRootShellVariant.allCases {
            for editorImplementation in ManifestInlineEditorImplementation.allCases {
                let baseline = try await hostSnapshot(
                    viewModel: makeDeploymentViewModel(deployment: deployment),
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialDeploymentInspectorTab: .overview,
                            shellVariant: shellVariant,
                            manifestInlineEditorImplementation: editorImplementation
                        )
                    },
                    section: .workloads,
                    kind: .deployment
                )
                let yaml = try await hostSnapshot(
                    viewModel: makeDeploymentViewModel(deployment: deployment),
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialDeploymentInspectorTab: .yaml,
                            shellVariant: shellVariant,
                            manifestInlineEditorImplementation: editorImplementation,
                            initialYAMLInlineEditing: editorImplementation.supportsInlineEditing
                        )
                    },
                    section: .workloads,
                    kind: .deployment
                )

                assertAligned(baseline: baseline, candidate: yaml)
            }
        }
    }

    func testNetworkingDescribeTabRemainsTopAligned() async throws {
        let service = ServiceSummary(name: "sample-service", namespace: "team-alpha", type: "ClusterIP", clusterIP: "10.0.0.10")
        for shellVariant in RuneRootShellVariant.allCases {
            for editorImplementation in ManifestInlineEditorImplementation.allCases {
                let baseline = try await hostSnapshot(
                    viewModel: makeServiceViewModel(service: service),
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialServiceInspectorTab: .overview,
                            shellVariant: shellVariant,
                            manifestInlineEditorImplementation: editorImplementation
                        )
                    },
                    section: .networking,
                    kind: .service
                )
                let describe = try await hostSnapshot(
                    viewModel: makeServiceViewModel(service: service),
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialServiceInspectorTab: .describe,
                            shellVariant: shellVariant,
                            manifestInlineEditorImplementation: editorImplementation
                        )
                    },
                    section: .networking,
                    kind: .service
                )

                assertAligned(baseline: baseline, candidate: describe)
            }
        }
    }

    func testNetworkingYAMLTabRemainsTopAligned() async throws {
        let service = ServiceSummary(name: "sample-service", namespace: "team-alpha", type: "ClusterIP", clusterIP: "10.0.0.10")
        for shellVariant in RuneRootShellVariant.allCases {
            for editorImplementation in ManifestInlineEditorImplementation.allCases {
                let baseline = try await hostSnapshot(
                    viewModel: makeServiceViewModel(service: service),
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialServiceInspectorTab: .overview,
                            shellVariant: shellVariant,
                            manifestInlineEditorImplementation: editorImplementation
                        )
                    },
                    section: .networking,
                    kind: .service
                )
                let yaml = try await hostSnapshot(
                    viewModel: makeServiceViewModel(service: service),
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialServiceInspectorTab: .yaml,
                            shellVariant: shellVariant,
                            manifestInlineEditorImplementation: editorImplementation,
                            initialYAMLInlineEditing: editorImplementation.supportsInlineEditing
                        )
                    },
                    section: .networking,
                    kind: .service
                )

                assertAligned(baseline: baseline, candidate: yaml)
            }
        }
    }

    func testConfigYAMLAndDescribeRemainTopAligned() async throws {
        let resource = ClusterResourceSummary(
            kind: .configMap,
            name: "orders-config",
            namespace: "example-backend",
            primaryText: "ConfigMap",
            secondaryText: "12 keys"
        )
        for shellVariant in RuneRootShellVariant.allCases {
            for editorImplementation in ManifestInlineEditorImplementation.allCases {
                let yaml = try await hostSnapshot(
                    viewModel: makeConfigViewModel(resource: resource),
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialGenericResourceManifestTab: .yaml,
                            shellVariant: shellVariant,
                            manifestInlineEditorImplementation: editorImplementation,
                            initialYAMLInlineEditing: editorImplementation.supportsInlineEditing
                        )
                    },
                    section: .config,
                    kind: .configMap,
                    settleNanoseconds: 250_000_000
                )
                let describe = try await hostSnapshot(
                    viewModel: makeConfigViewModel(resource: resource),
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialGenericResourceManifestTab: .describe,
                            shellVariant: shellVariant,
                            manifestInlineEditorImplementation: editorImplementation
                        )
                    },
                    section: .config,
                    kind: .configMap,
                    settleNanoseconds: 250_000_000
                )

                assertAligned(baseline: yaml, candidate: describe)
            }
        }
    }

    func testAppKitSplitViewRestoresPersistedSidebarWidthOnLaunch() async throws {
        let pod = PodSummary(name: "sample-pod-7c9db", namespace: "team-alpha", status: "Running", totalRestarts: 1, ageDescription: "5m")

        let baseline = try await hostAppKitSplitSnapshot(
            viewModel: makePodViewModel(pod: pod),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview,
                    shellVariant: .appKitSplitView,
                    manifestInlineEditorImplementation: .swiftUITextEditor,
                    initialSidebarWidthOverride: 280,
                    initialDetailWidthOverride: 440
                )
            },
            section: .workloads,
            kind: .pod,
            settleNanoseconds: 400_000_000
        )

        let widerSidebar = try await hostAppKitSplitSnapshot(
            viewModel: makePodViewModel(pod: pod),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview,
                    shellVariant: .appKitSplitView,
                    manifestInlineEditorImplementation: .swiftUITextEditor,
                    initialSidebarWidthOverride: 360,
                    initialDetailWidthOverride: 440
                )
            },
            section: .workloads,
            kind: .pod,
            settleNanoseconds: 400_000_000
        )

        XCTAssertEqual(
            widerSidebar.sidebarWidth - baseline.sidebarWidth,
            80,
            accuracy: 10,
            "baseline=\(baseline) wider=\(widerSidebar)"
        )
        XCTAssertEqual(
            widerSidebar.contentMinX - baseline.contentMinX,
            80,
            accuracy: 10,
            "baseline=\(baseline) wider=\(widerSidebar)"
        )
        XCTAssertEqual(
            widerSidebar.detailMinX,
            baseline.detailMinX,
            accuracy: 10,
            "baseline=\(baseline) wider=\(widerSidebar)"
        )
    }

    func testAppKitSplitViewRestoresPersistedDetailWidthOnLaunch() async throws {
        let pod = PodSummary(name: "sample-pod-7c9db", namespace: "team-alpha", status: "Running", totalRestarts: 1, ageDescription: "5m")

        let baseline = try await hostAppKitSplitSnapshot(
            viewModel: makePodViewModel(pod: pod),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview,
                    shellVariant: .appKitSplitView,
                    manifestInlineEditorImplementation: .swiftUITextEditor,
                    initialSidebarWidthOverride: 280,
                    initialDetailWidthOverride: 440
                )
            },
            section: .workloads,
            kind: .pod,
            settleNanoseconds: 400_000_000
        )

        let widerDetail = try await hostAppKitSplitSnapshot(
            viewModel: makePodViewModel(pod: pod),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview,
                    shellVariant: .appKitSplitView,
                    manifestInlineEditorImplementation: .swiftUITextEditor,
                    initialSidebarWidthOverride: 280,
                    initialDetailWidthOverride: 520
                )
            },
            section: .workloads,
            kind: .pod,
            settleNanoseconds: 400_000_000
        )

        XCTAssertEqual(
            widerDetail.contentMinX,
            baseline.contentMinX,
            accuracy: 10,
            "baseline=\(baseline) wider=\(widerDetail)"
        )
        XCTAssertEqual(
            widerDetail.detailWidth - baseline.detailWidth,
            80,
            accuracy: 10,
            "baseline=\(baseline) wider=\(widerDetail)"
        )
        XCTAssertEqual(
            widerDetail.detailMinX - baseline.detailMinX,
            -80,
            accuracy: 10,
            "baseline=\(baseline) wider=\(widerDetail)"
        )
    }

    func testConfigAndWorkloadsRemainTopAligned() async throws {
        for shellVariant in RuneRootShellVariant.allCases {
            let viewModel = RuneAppViewModel()
            var snapshots: [RuneRootLayoutSnapshot] = []

            let host = NSHostingController(
                rootView: RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: { snapshot in
                        snapshots.append(snapshot)
                    },
                    debugDisableBootstrap: true,
                    shellVariant: shellVariant
                )
            )

            let window = NSWindow(
                contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
                styleMask: [.titled, .closable, .resizable],
                backing: .buffered,
                defer: false
            )
            window.contentViewController = host
            window.makeKeyAndOrderFront(nil)
            defer { window.orderOut(nil) }

            viewModel.state.selectedSection = .workloads
            viewModel.state.selectedWorkloadKind = .pod
            let workloadsSnapshot = try await waitForSnapshot(in: window, snapshots: { snapshots }) {
                $0.section == .workloads
                    && $0.workloadKind == .pod
                    && $0.contentMinY != nil
                    && $0.headerMinY != nil
                    && $0.detailMinY != nil
                    && $0.contentMinX != nil
                    && $0.headerMinX != nil
                    && $0.detailMinX != nil
            }

            viewModel.state.selectedSection = .config
            viewModel.state.selectedWorkloadKind = .configMap
            let configSnapshot = try await waitForSnapshot(in: window, snapshots: { snapshots }) {
                $0.section == .config
                    && $0.workloadKind == .configMap
                    && $0.contentMinY != nil
                    && $0.headerMinY != nil
                    && $0.detailMinY != nil
                    && $0.contentMinX != nil
                    && $0.headerMinX != nil
                    && $0.detailMinX != nil
            }

            XCTAssertEqual(
                workloadsSnapshot.resolvedWindowTopInset,
                configSnapshot.resolvedWindowTopInset,
                accuracy: 0.5
            )
            XCTAssertEqual(workloadsSnapshot.contentMinY ?? 0, configSnapshot.contentMinY ?? 0, accuracy: verticalAlignmentAccuracy)
            XCTAssertEqual(workloadsSnapshot.headerMinY ?? 0, configSnapshot.headerMinY ?? 0, accuracy: verticalAlignmentAccuracy)
            XCTAssertEqual(workloadsSnapshot.detailMinY ?? 0, configSnapshot.detailMinY ?? 0, accuracy: verticalAlignmentAccuracy)
            XCTAssertEqual(workloadsSnapshot.contentMinX ?? 0, configSnapshot.contentMinX ?? 0, accuracy: horizontalOffsetAccuracy, "content column horizontal offset between sections")
            XCTAssertEqual(workloadsSnapshot.headerMinX ?? 0, configSnapshot.headerMinX ?? 0, accuracy: horizontalOffsetAccuracy, "header horizontal offset between sections")
            XCTAssertEqual(workloadsSnapshot.detailMinX ?? 0, configSnapshot.detailMinX ?? 0, accuracy: horizontalOffsetAccuracy, "detail column horizontal offset between sections")
        }
    }

    func testSectionTransitionsDoNotDriftAfterLayoutSettles() async throws {
        for shellVariant in RuneRootShellVariant.allCases {
            let viewModel = RuneAppViewModel()
            var snapshots: [RuneRootLayoutSnapshot] = []

            let host = NSHostingController(
                rootView: RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: { snapshot in
                        snapshots.append(snapshot)
                    },
                    debugDisableBootstrap: true,
                    shellVariant: shellVariant
                )
            )

            let window = NSWindow(
                contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
                styleMask: [.titled, .closable, .resizable],
                backing: .buffered,
                defer: false
            )
            window.contentViewController = host
            window.makeKeyAndOrderFront(nil)
            defer { window.orderOut(nil) }

            try await assertStableTransition(
                in: window,
                snapshots: { snapshots },
                viewModel: viewModel,
                section: .workloads,
                kind: .pod
            )
            try await assertStableTransition(
                in: window,
                snapshots: { snapshots },
                viewModel: viewModel,
                section: .config,
                kind: .configMap
            )
            try await assertStableTransition(
                in: window,
                snapshots: { snapshots },
                viewModel: viewModel,
                section: .rbac,
                kind: .role
            )
            try await assertStableTransition(
                in: window,
                snapshots: { snapshots },
                viewModel: viewModel,
                section: .workloads,
                kind: .pod
            )
        }
    }

    func testPodSectionDoesNotDriftWhenKeptOpenLonger() async throws {
        let pod = PodSummary(name: "sample-pod-7c9db", namespace: "team-alpha", status: "Running", totalRestarts: 1, ageDescription: "5m")

        for shellVariant in RuneRootShellVariant.allCases {
            let viewModel = makePodViewModel(pod: pod)
            var snapshots: [RuneRootLayoutSnapshot] = []

            let host = NSHostingController(
                rootView: RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: { snapshot in
                        snapshots.append(snapshot)
                    },
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview,
                    shellVariant: shellVariant
                )
            )

            let window = NSWindow(
                contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
                styleMask: [.titled, .closable, .resizable],
                backing: .buffered,
                defer: false
            )
            window.contentViewController = host
            window.makeKeyAndOrderFront(nil)
            defer { window.orderOut(nil) }

            try await assertStableTransition(
                in: window,
                snapshots: { snapshots },
                viewModel: viewModel,
                section: .workloads,
                kind: .pod,
                observationDuration: Self.longPodPostSettleObservationDuration
            )
        }
    }

    func testPodLayoutDoesNotDriftWhenLoadingFinishes() async throws {
        let basePod = PodSummary(
            name: "sample-pod-7c9db",
            namespace: "team-alpha",
            status: "Running",
            totalRestarts: 1,
            ageDescription: "5m"
        )
        let enrichedPod = PodSummary(
            name: basePod.name,
            namespace: basePod.namespace,
            status: basePod.status,
            totalRestarts: basePod.totalRestarts,
            ageDescription: basePod.ageDescription,
            cpuUsage: "12m",
            memoryUsage: "64Mi",
            podIP: "10.42.0.15",
            hostIP: "10.0.0.24",
            nodeName: "aks-nodepool-001",
            qosClass: "Burstable",
            containersReady: "2/2",
            containerNamesLine: "web, sidecar"
        )

        for shellVariant in RuneRootShellVariant.allCases {
            let state = RuneAppState()
            state.selectedSection = .workloads
            state.selectedWorkloadKind = .pod
            state.selectedNamespace = basePod.namespace
            state.isLoading = true

            let viewModel = RuneAppViewModel(state: state)
            var snapshots: [RuneRootLayoutSnapshot] = []

            let host = NSHostingController(
                rootView: RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: { snapshot in
                        snapshots.append(snapshot)
                    },
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview,
                    shellVariant: shellVariant
                )
            )

            let window = NSWindow(
                contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
                styleMask: [.titled, .closable, .resizable],
                backing: .buffered,
                defer: false
            )
            window.contentViewController = host
            window.makeKeyAndOrderFront(nil)
            defer { window.orderOut(nil) }

            let loadingSnapshot = try await stableSnapshot(
                in: window,
                snapshots: { snapshots },
                section: .workloads,
                kind: .pod,
                observationDuration: 0.4
            )

            state.setPods(makePodLoadingRows(seed: basePod, count: 36))
            state.isLoading = false
            state.beginResourceDetailLoad()
            state.setSelectedPod(basePod.mergingInspectorDetail(enrichedPod))
            state.setResourceYAML(sampleYAML(named: basePod.name))
            state.setResourceDescribe(sampleDescribe(named: basePod.name))
            state.finishResourceDetailLoad()

            let loadedSnapshot = try await stableSnapshot(
                in: window,
                snapshots: { snapshots },
                section: .workloads,
                kind: .pod,
                observationDuration: 0.8
            )

            XCTAssertEqual(
                loadedSnapshot.contentMinY ?? 0,
                loadingSnapshot.contentMinY ?? 0,
                accuracy: 2.0,
                "content MinY changed after pod load for shell=\(shellVariant.debugLabel)"
            )
            XCTAssertEqual(
                loadedSnapshot.detailMinY ?? 0,
                loadingSnapshot.detailMinY ?? 0,
                accuracy: 2.0,
                "detail MinY changed after pod load for shell=\(shellVariant.debugLabel)"
            )
            XCTAssertEqual(
                loadedSnapshot.contentMinX ?? 0,
                loadingSnapshot.contentMinX ?? 0,
                accuracy: horizontalOffsetAccuracy,
                "content MinX changed after pod load for shell=\(shellVariant.debugLabel)"
            )
            XCTAssertEqual(
                loadedSnapshot.detailMinX ?? 0,
                loadingSnapshot.detailMinX ?? 0,
                accuracy: horizontalOffsetAccuracy,
                "detail MinX changed after pod load for shell=\(shellVariant.debugLabel)"
            )

            try await assertSnapshotsRemainStable(
                in: window,
                snapshots: { snapshots },
                section: .workloads,
                kind: .pod,
                baseline: loadedSnapshot,
                settledSnapshotCount: snapshots.count,
                observationDuration: 1.5
            )
        }
    }

    func testEventGoToResourceDoesNotDriftOrLeaveEventsBeforeTargetLoads() async throws {
        let event = EventSummary(
            type: "Warning",
            reason: "FailedComputeMetricsReplicas",
            objectName: "aurora-signal-weaver",
            message: "invalid metrics (1 invalid out of 1)",
            lastTimestamp: "2026-04-21T22:21:50Z",
            involvedKind: "HorizontalPodAutoscaler",
            involvedNamespace: "delta-zone"
        )
        let secondaryEvent = EventSummary(
            type: "Warning",
            reason: "FailedGetResourceMetric",
            objectName: "aurora-signal-weaver",
            message: "failed to get cpu utilization",
            lastTimestamp: "2026-04-21T22:21:40Z",
            involvedKind: "HorizontalPodAutoscaler",
            involvedNamespace: "delta-zone"
        )
        let autoscaler = ClusterResourceSummary(
            kind: .horizontalPodAutoscaler,
            name: event.objectName,
            namespace: "delta-zone",
            primaryText: "Scale target: Deployment/aurora-signal-weaver",
            secondaryText: "Min 1 / Max 5"
        )

        for shellVariant in RuneRootShellVariant.allCases {
            let state = RuneAppState()
            state.selectedSection = .events
            state.selectedWorkloadKind = .event
            state.selectedNamespace = "delta-zone"
            state.setEvents([secondaryEvent, event])
            state.setSelectedEvent(event)

            let viewModel = RuneAppViewModel(state: state)
            var snapshots: [RuneRootLayoutSnapshot] = []

            let host = NSHostingController(
                rootView: RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: { snapshot in
                        snapshots.append(snapshot)
                    },
                    debugDisableBootstrap: true,
                    shellVariant: shellVariant
                )
            )

            let window = NSWindow(
                contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
                styleMask: [.titled, .closable, .resizable],
                backing: .buffered,
                defer: false
            )
            window.contentViewController = host
            window.makeKeyAndOrderFront(nil)
            defer { window.orderOut(nil) }

            let baseline = try await stableSnapshot(
                in: window,
                snapshots: { snapshots },
                section: .events,
                kind: .event,
                observationDuration: 0.35
            )

            viewModel.openEventSource(event)

            let stillEvents = try await stableSnapshot(
                in: window,
                snapshots: { snapshots },
                section: .events,
                kind: .event,
                observationDuration: 0.45
            )
            assertAligned(baseline: baseline, candidate: stillEvents)

            state.setHorizontalPodAutoscalers([autoscaler])
            viewModel.openEventSource(event)

            let navigated = try await stableSnapshot(
                in: window,
                snapshots: { snapshots },
                section: .workloads,
                kind: .horizontalPodAutoscaler,
                observationDuration: 0.6
            )

            XCTAssertEqual(
                navigated.contentMinY ?? 0,
                baseline.contentMinY ?? 0,
                accuracy: 2.0,
                "content MinY changed after event source navigation for shell=\(shellVariant.debugLabel)"
            )
            XCTAssertEqual(
                navigated.detailMinY ?? 0,
                baseline.detailMinY ?? 0,
                accuracy: 2.0,
                "detail MinY changed after event source navigation for shell=\(shellVariant.debugLabel)"
            )
            XCTAssertEqual(
                navigated.contentMinX ?? 0,
                baseline.contentMinX ?? 0,
                accuracy: horizontalOffsetAccuracy,
                "content MinX changed after event source navigation for shell=\(shellVariant.debugLabel)"
            )
            XCTAssertEqual(
                navigated.detailMinX ?? 0,
                baseline.detailMinX ?? 0,
                accuracy: horizontalOffsetAccuracy,
                "detail MinX changed after event source navigation for shell=\(shellVariant.debugLabel)"
            )
            XCTAssertEqual(viewModel.state.selectedHorizontalPodAutoscaler?.name, autoscaler.name)
        }
    }

    func testSwitchingToPodsAndHydratingLargeListDoesNotDrift() async throws {
        let targetPodName = "signal-weaver-5f8f6d7c9b-r4m2k"
        let targetNamespace = "delta-zone"
        let selectedEvent = EventSummary(
            type: "Normal",
            reason: "Created",
            objectName: targetPodName,
            message: "Created container signal-weaver",
            lastTimestamp: "2026-04-21T22:21:50Z",
            involvedKind: "Pod",
            involvedNamespace: targetNamespace
        )
        let loadingSeed = PodSummary(
            name: targetPodName,
            namespace: targetNamespace,
            status: "Running",
            totalRestarts: 0,
            ageDescription: "7h"
        )
        let enrichedSelection = PodSummary(
            name: targetPodName,
            namespace: targetNamespace,
            status: "Running",
            totalRestarts: 0,
            ageDescription: "7h",
            cpuUsage: "1m",
            memoryUsage: "0Mi",
            podIP: "10.42.0.56",
            hostIP: "192.168.16.3",
            nodeName: "node-spark-1",
            qosClass: "BestEffort",
            containersReady: "1/1",
            containerNamesLine: "signal-weaver"
        )

        for shellVariant in RuneRootShellVariant.allCases {
            let state = RuneAppState()
            state.selectedSection = .events
            state.selectedWorkloadKind = .event
            state.selectedNamespace = targetNamespace
            state.setEvents([selectedEvent])
            state.setSelectedEvent(selectedEvent)

            let viewModel = RuneAppViewModel(state: state)
            var snapshots: [RuneRootLayoutSnapshot] = []

            let host = NSHostingController(
                rootView: RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: { snapshot in
                        snapshots.append(snapshot)
                    },
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview,
                    shellVariant: shellVariant
                )
            )

            let window = NSWindow(
                contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
                styleMask: [.titled, .closable, .resizable],
                backing: .buffered,
                defer: false
            )
            window.contentViewController = host
            window.makeKeyAndOrderFront(nil)
            defer { window.orderOut(nil) }

            let eventsSnapshot = try await stableSnapshot(
                in: window,
                snapshots: { snapshots },
                section: .events,
                kind: .event,
                observationDuration: 0.35
            )

            state.selectedSection = .workloads
            state.selectedWorkloadKind = .pod
            state.isLoading = true

            let loadingSnapshot = try await stableSnapshot(
                in: window,
                snapshots: { snapshots },
                section: .workloads,
                kind: .pod,
                observationDuration: 0.45
            )
            assertAligned(baseline: eventsSnapshot, candidate: loadingSnapshot)

            let firstWave = makePodLoadingRows(seed: loadingSeed, count: 120)
            state.setPods(firstWave)
            state.setSelectedPod(firstWave.first(where: { $0.name == targetPodName }))
            state.beginResourceDetailLoad()
            state.setResourceYAML(sampleYAML(named: targetPodName))
            state.setResourceDescribe(sampleDescribe(named: targetPodName))
            state.finishResourceDetailLoad()
            state.isLoading = false

            let hydratedSnapshot = try await stableSnapshot(
                in: window,
                snapshots: { snapshots },
                section: .workloads,
                kind: .pod,
                observationDuration: 0.9
            )

            XCTAssertEqual(
                hydratedSnapshot.contentMinY ?? 0,
                loadingSnapshot.contentMinY ?? 0,
                accuracy: 2.0,
                "content MinY changed after first pod hydration wave for shell=\(shellVariant.debugLabel)"
            )
            XCTAssertEqual(
                hydratedSnapshot.detailMinY ?? 0,
                loadingSnapshot.detailMinY ?? 0,
                accuracy: 2.0,
                "detail MinY changed after first pod hydration wave for shell=\(shellVariant.debugLabel)"
            )
            XCTAssertEqual(
                hydratedSnapshot.contentMinX ?? 0,
                loadingSnapshot.contentMinX ?? 0,
                accuracy: horizontalOffsetAccuracy,
                "content MinX changed after first pod hydration wave for shell=\(shellVariant.debugLabel)"
            )
            XCTAssertEqual(
                hydratedSnapshot.detailMinX ?? 0,
                loadingSnapshot.detailMinX ?? 0,
                accuracy: horizontalOffsetAccuracy,
                "detail MinX changed after first pod hydration wave for shell=\(shellVariant.debugLabel)"
            )

            let secondWave = firstWave.enumerated().map { index, pod in
                index == 0 ? pod.mergingInspectorDetail(enrichedSelection) : pod
            }
            state.setPods(secondWave)
            state.beginResourceDetailLoad()
            state.setSelectedPod(secondWave.first(where: { $0.name == targetPodName }))
            state.setResourceYAML(sampleYAML(named: targetPodName) + "\n# enriched")
            state.setResourceDescribe(sampleDescribe(named: targetPodName) + "\nNode: node-spark-1")
            state.finishResourceDetailLoad()

            let enrichedSnapshot = try await stableSnapshot(
                in: window,
                snapshots: { snapshots },
                section: .workloads,
                kind: .pod,
                observationDuration: 1.0
            )

            XCTAssertEqual(
                enrichedSnapshot.contentMinY ?? 0,
                loadingSnapshot.contentMinY ?? 0,
                accuracy: 2.0,
                "content MinY changed after second pod hydration wave for shell=\(shellVariant.debugLabel)"
            )
            XCTAssertEqual(
                enrichedSnapshot.detailMinY ?? 0,
                loadingSnapshot.detailMinY ?? 0,
                accuracy: 2.0,
                "detail MinY changed after second pod hydration wave for shell=\(shellVariant.debugLabel)"
            )
            XCTAssertEqual(
                enrichedSnapshot.contentMinX ?? 0,
                loadingSnapshot.contentMinX ?? 0,
                accuracy: horizontalOffsetAccuracy,
                "content MinX changed after second pod hydration wave for shell=\(shellVariant.debugLabel)"
            )
            XCTAssertEqual(
                enrichedSnapshot.detailMinX ?? 0,
                loadingSnapshot.detailMinX ?? 0,
                accuracy: horizontalOffsetAccuracy,
                "detail MinX changed after second pod hydration wave for shell=\(shellVariant.debugLabel)"
            )

            try await assertSnapshotsRemainStable(
                in: window,
                snapshots: { snapshots },
                section: .workloads,
                kind: .pod,
                baseline: enrichedSnapshot,
                settledSnapshotCount: snapshots.count,
                observationDuration: 1.6
            )
        }
    }

    private func waitForSnapshot(
        in window: NSWindow,
        snapshots: @escaping () -> [RuneRootLayoutSnapshot],
        matching predicate: (RuneRootLayoutSnapshot) -> Bool
    ) async throws -> RuneRootLayoutSnapshot {
        let timeout = Date().addingTimeInterval(2.0)
        while Date() < timeout {
            window.contentView?.layoutSubtreeIfNeeded()
            await Task.yield()

            if let snapshot = snapshots().last(where: predicate) {
                return snapshot
            }

            try await Task.sleep(nanoseconds: 20_000_000)
        }

        XCTFail("Timed out waiting for layout snapshot")
        throw CancellationError()
    }

    private func stableSnapshot(
        in window: NSWindow,
        snapshots: @escaping () -> [RuneRootLayoutSnapshot],
        section: RuneSection,
        kind: KubeResourceKind,
        observationDuration: TimeInterval
    ) async throws -> RuneRootLayoutSnapshot {
        let settled = try await waitForSnapshot(in: window, snapshots: snapshots) {
            $0.section == section
                && $0.workloadKind == kind
                && $0.contentMinY != nil
                && $0.headerMinY != nil
                && $0.detailMinY != nil
                && $0.contentMinX != nil
                && $0.headerMinX != nil
                && $0.detailMinX != nil
        }

        let settledSnapshotCount = snapshots().count
        try await assertSnapshotsRemainStable(
            in: window,
            snapshots: snapshots,
            section: section,
            kind: kind,
            baseline: settled,
            settledSnapshotCount: settledSnapshotCount,
            observationDuration: observationDuration
        )

        return snapshots().last(where: {
            $0.section == section
                && $0.workloadKind == kind
                && $0.contentMinY != nil
                && $0.headerMinY != nil
                && $0.detailMinY != nil
                && $0.contentMinX != nil
                && $0.headerMinX != nil
                && $0.detailMinX != nil
        }) ?? settled
    }

    private func assertStableTransition(
        in window: NSWindow,
        snapshots: @escaping () -> [RuneRootLayoutSnapshot],
        viewModel: RuneAppViewModel,
        section: RuneSection,
        kind: KubeResourceKind,
        observationDuration: TimeInterval = RuneRootViewLayoutRegressionTests.defaultPostSettleObservationDuration
    ) async throws {
        viewModel.state.selectedSection = section
        viewModel.state.selectedWorkloadKind = kind

        let settled = try await waitForSnapshot(in: window, snapshots: snapshots) {
            $0.section == section
                && $0.workloadKind == kind
                && $0.contentMinY != nil
                && $0.headerMinY != nil
                && $0.detailMinY != nil
                && $0.contentMinX != nil
                && $0.headerMinX != nil
                && $0.detailMinX != nil
        }

        let settledSnapshotCount = snapshots().count
        let baselineContentY = settled.contentMinY ?? 0
        let baselineDetailY = settled.detailMinY ?? 0
        let baselineContentX = settled.contentMinX ?? 0
        let baselineDetailX = settled.detailMinX ?? 0
        try await assertSnapshotsRemainStable(
            in: window,
            snapshots: snapshots,
            section: section,
            kind: kind,
            baseline: RuneRootLayoutSnapshot(
                section: section,
                workloadKind: kind,
                measuredWindowTopInset: settled.measuredWindowTopInset,
                resolvedWindowTopInset: settled.resolvedWindowTopInset,
                contentMinY: baselineContentY,
                headerMinY: settled.headerMinY,
                detailMinY: baselineDetailY,
                contentMinX: baselineContentX,
                headerMinX: settled.headerMinX,
                detailMinX: baselineDetailX
            ),
            settledSnapshotCount: settledSnapshotCount,
            observationDuration: observationDuration
        )
    }

    private func assertSnapshotsRemainStable(
        in window: NSWindow,
        snapshots: @escaping () -> [RuneRootLayoutSnapshot],
        section: RuneSection,
        kind: KubeResourceKind,
        baseline: RuneRootLayoutSnapshot,
        settledSnapshotCount: Int,
        observationDuration: TimeInterval
    ) async throws {
        let timeout = Date().addingTimeInterval(observationDuration)

        while Date() < timeout {
            window.contentView?.layoutSubtreeIfNeeded()
            await Task.yield()

            let recentSnapshots = snapshots().dropFirst(settledSnapshotCount).filter {
                $0.section == section
                    && $0.workloadKind == kind
                    && $0.contentMinY != nil
                    && $0.headerMinY != nil
                    && $0.detailMinY != nil
                    && $0.contentMinX != nil
                    && $0.headerMinX != nil
                    && $0.detailMinX != nil
            }

            for snapshot in recentSnapshots.suffix(6) {
                XCTAssertEqual(snapshot.contentMinY ?? 0, baseline.contentMinY ?? 0, accuracy: 2.0)
                XCTAssertEqual(snapshot.detailMinY ?? 0, baseline.detailMinY ?? 0, accuracy: 2.0)
                XCTAssertEqual(
                    snapshot.contentMinX ?? 0,
                    baseline.contentMinX ?? 0,
                    accuracy: horizontalOffsetAccuracy,
                    "post-settle content MinX offset"
                )
                XCTAssertEqual(
                    snapshot.detailMinX ?? 0,
                    baseline.detailMinX ?? 0,
                    accuracy: horizontalOffsetAccuracy,
                    "post-settle detail MinX offset"
                )
                XCTAssertGreaterThanOrEqual((snapshot.headerMinY ?? 0) - (snapshot.contentMinY ?? 0), 12.0)
                XCTAssertLessThanOrEqual((snapshot.headerMinY ?? 0) - (snapshot.contentMinY ?? 0), 64.0)
            }

            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }

    private func makeServiceViewModel(service: ServiceSummary) -> RuneAppViewModel {
        let state = RuneAppState()
        state.selectedSection = .networking
        state.selectedWorkloadKind = .service
        state.selectedNamespace = service.namespace
        state.setServices([service])
        state.selectedService = service
        state.setResourceYAML(sampleYAML(named: service.name))
        state.setResourceDescribe(sampleDescribe(named: service.name))
        return RuneAppViewModel(state: state)
    }

    private func makePodViewModel(pod: PodSummary) -> RuneAppViewModel {
        let state = RuneAppState()
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.selectedNamespace = pod.namespace
        state.setPods([pod])
        state.selectedPod = pod
        state.setResourceYAML(sampleYAML(named: pod.name))
        state.setResourceDescribe(sampleDescribe(named: pod.name))
        return RuneAppViewModel(state: state)
    }

    private func makeDeploymentViewModel(deployment: DeploymentSummary) -> RuneAppViewModel {
        let state = RuneAppState()
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .deployment
        state.selectedNamespace = deployment.namespace
        state.setDeployments([deployment])
        state.selectedDeployment = deployment
        state.setResourceYAML(sampleYAML(named: deployment.name))
        state.setResourceDescribe(sampleDescribe(named: deployment.name))
        return RuneAppViewModel(state: state)
    }

    private func makeConfigViewModel(resource: ClusterResourceSummary) -> RuneAppViewModel {
        let state = RuneAppState()
        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.selectedNamespace = resource.namespace ?? "default"
        state.setConfigMaps([resource])
        state.selectedConfigMap = resource
        state.setResourceYAML(sampleYAML(named: resource.name))
        state.setResourceDescribe(sampleDescribe(named: resource.name))
        return RuneAppViewModel(state: state)
    }

    private func hostSnapshot(
        viewModel: RuneAppViewModel,
        rootView: (RuneAppViewModel, @escaping (RuneRootLayoutSnapshot) -> Void) -> RuneRootView,
        section: RuneSection,
        kind: KubeResourceKind,
        sidebarWidth: Double = 280,
        detailWidth: Double = 440,
        settleNanoseconds: UInt64 = 0
    ) async throws -> RuneRootLayoutSnapshot {
        let defaults = UserDefaults.standard
        let sidebarKey = RuneSettingsKeys.layoutSidebarWidth
        let detailKey = RuneSettingsKeys.layoutDetailWidth
        let originalSidebar = defaults.object(forKey: sidebarKey)
        let originalDetail = defaults.object(forKey: detailKey)

        defaults.set(sidebarWidth, forKey: sidebarKey)
        defaults.set(detailWidth, forKey: detailKey)

        defer {
            if let originalSidebar {
                defaults.set(originalSidebar, forKey: sidebarKey)
            } else {
                defaults.removeObject(forKey: sidebarKey)
            }

            if let originalDetail {
                defaults.set(originalDetail, forKey: detailKey)
            } else {
                defaults.removeObject(forKey: detailKey)
            }
        }

        var snapshots: [RuneRootLayoutSnapshot] = []
        let host = NSHostingController(
            rootView: rootView(viewModel) { snapshot in
                snapshots.append(snapshot)
            }
        )

        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        host.view.frame = NSRect(x: 0, y: 0, width: 1440, height: 900)
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        let snapshot = try await waitForSnapshot(in: window, snapshots: { snapshots }) {
            $0.section == section
                && $0.workloadKind == kind
                && $0.contentMinY != nil
                && $0.headerMinY != nil
                && $0.detailMinY != nil
                && $0.contentMinX != nil
                && $0.headerMinX != nil
                && $0.detailMinX != nil
        }

        guard settleNanoseconds > 0 else {
            return snapshot
        }

        try await Task.sleep(nanoseconds: settleNanoseconds)

        if let settled = snapshots.last(where: {
            $0.section == section
                && $0.workloadKind == kind
                && $0.contentMinY != nil
                && $0.headerMinY != nil
                && $0.detailMinY != nil
                && $0.contentMinX != nil
                && $0.headerMinX != nil
                && $0.detailMinX != nil
        }) {
            return settled
        }

        return snapshot
    }

    private struct AppKitSplitLaunchSnapshot {
        let splitViewWidth: CGFloat
        let windowContentWidth: CGFloat
        let sidebarWidth: CGFloat
        let contentMinX: CGFloat
        let detailMinX: CGFloat
        let detailWidth: CGFloat
    }

    private func hostAppKitSplitSnapshot(
        viewModel: RuneAppViewModel,
        rootView: (RuneAppViewModel, @escaping (RuneRootLayoutSnapshot) -> Void) -> RuneRootView,
        section: RuneSection,
        kind: KubeResourceKind,
        settleNanoseconds: UInt64 = 0
    ) async throws -> AppKitSplitLaunchSnapshot {
        let host = NSHostingController(
            rootView: rootView(viewModel) { _ in }
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        let container = NSView(frame: NSRect(x: 0, y: 0, width: 1440, height: 900))
        window.contentView = container
        let hostView = host.view
        hostView.frame = container.bounds
        hostView.autoresizingMask = [.width, .height]
        container.addSubview(hostView)
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }
        container.layoutSubtreeIfNeeded()

        guard settleNanoseconds > 0 else {
            return try await waitForAppKitSplitSnapshot(in: container)
        }

        try await Task.sleep(nanoseconds: settleNanoseconds)
        return try await waitForAppKitSplitSnapshot(in: container)
    }

    private func waitForAppKitSplitSnapshot(in rootView: NSView) async throws -> AppKitSplitLaunchSnapshot {
        let timeout = Date().addingTimeInterval(2.0)
        while Date() < timeout {
            rootView.layoutSubtreeIfNeeded()
            await Task.yield()

            if let splitView = firstThreePaneVerticalSplitView(in: rootView),
               splitView.arrangedSubviews.count == 3 {
                let sidebar = splitView.arrangedSubviews[0].frame
                let content = splitView.arrangedSubviews[1].frame
                let detail = splitView.arrangedSubviews[2].frame

                if sidebar.width > 1, content.width > 1, detail.width > 1 {
                    return AppKitSplitLaunchSnapshot(
                        splitViewWidth: splitView.frame.width,
                        windowContentWidth: rootView.bounds.width,
                        sidebarWidth: sidebar.width,
                        contentMinX: content.minX,
                        detailMinX: detail.minX,
                        detailWidth: detail.width
                    )
                }
            }

            try await Task.sleep(nanoseconds: 20_000_000)
        }

        XCTFail("Timed out waiting for AppKit split metrics")
        throw CancellationError()
    }

    private func firstThreePaneVerticalSplitView(in view: NSView?) -> NSSplitView? {
        guard let view else { return nil }
        if let splitView = view as? NSSplitView,
           splitView.isVertical,
           splitView.arrangedSubviews.count == 3 {
            return splitView
        }

        for subview in view.subviews {
            if let splitView = firstThreePaneVerticalSplitView(in: subview) {
                return splitView
            }
        }

        return nil
    }

    private func assertAligned(baseline: RuneRootLayoutSnapshot, candidate: RuneRootLayoutSnapshot) {
        XCTAssertEqual(
            baseline.resolvedWindowTopInset,
            candidate.resolvedWindowTopInset,
            accuracy: 0.5
        )
        XCTAssertEqual(baseline.contentMinY ?? 0, candidate.contentMinY ?? 0, accuracy: verticalAlignmentAccuracy, "content top (minY) drift")
        XCTAssertEqual(baseline.headerMinY ?? 0, candidate.headerMinY ?? 0, accuracy: verticalAlignmentAccuracy, "header top (minY) drift")
        XCTAssertEqual(baseline.detailMinY ?? 0, candidate.detailMinY ?? 0, accuracy: detailVerticalAlignmentAccuracy, "detail top (minY) drift")
        XCTAssertEqual(baseline.contentMinX ?? 0, candidate.contentMinX ?? 0, accuracy: horizontalOffsetAccuracy, "content leading edge offset (minX)")
        XCTAssertEqual(baseline.headerMinX ?? 0, candidate.headerMinX ?? 0, accuracy: horizontalOffsetAccuracy, "header leading edge offset (minX)")
        XCTAssertEqual(baseline.detailMinX ?? 0, candidate.detailMinX ?? 0, accuracy: horizontalOffsetAccuracy, "detail leading edge offset (minX) — catches right panel jumping sideways")
    }

    private func sampleYAML(named name: String) -> String {
        Array(repeating: "kind: ConfigMap\nmetadata:\n  name: \(name)\n  namespace: team-alpha\n", count: 24)
            .joined(separator: "---\n")
    }

    private func sampleDescribe(named name: String) -> String {
        Array(repeating: "Name: \(name)\nNamespace: team-alpha\nLabels: app=sample\nEvents: <none>\n", count: 32)
            .joined(separator: "\n")
    }

    private func makePodLoadingRows(seed: PodSummary, count: Int) -> [PodSummary] {
        (0..<count).map { index in
            PodSummary(
                name: index == 0 ? seed.name : "\(seed.name)-\(index)",
                namespace: seed.namespace,
                status: index.isMultiple(of: 2) ? "Running" : "Succeeded",
                totalRestarts: index % 3,
                ageDescription: "\(index + 1)m",
                cpuUsage: "\(12 + index)m",
                memoryUsage: "\(64 + index)Mi"
            )
        }
    }
}
