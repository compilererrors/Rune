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
    /// Horizontal probe alignment — catches inspector/content shifting sideways (“offset”) when tabs or editors change.
    private let horizontalOffsetAccuracy: CGFloat = 2.5

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
                    kind: .configMap
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
                    kind: .configMap
                )

                assertAligned(baseline: yaml, candidate: describe)
            }
        }
    }

    func testAppKitSplitViewRestoresPersistedSidebarWidthOnLaunch() async throws {
        let pod = PodSummary(name: "sample-pod-7c9db", namespace: "team-alpha", status: "Running", totalRestarts: 1, ageDescription: "5m")

        let baseline = try await hostSnapshot(
            viewModel: makePodViewModel(pod: pod),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview,
                    shellVariant: .appKitSplitView,
                    manifestInlineEditorImplementation: .swiftUITextEditor
                )
            },
            section: .workloads,
            kind: .pod,
            sidebarWidth: 280,
            detailWidth: 440,
            settleNanoseconds: 400_000_000
        )

        let widerSidebar = try await hostSnapshot(
            viewModel: makePodViewModel(pod: pod),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview,
                    shellVariant: .appKitSplitView,
                    manifestInlineEditorImplementation: .swiftUITextEditor
                )
            },
            section: .workloads,
            kind: .pod,
            sidebarWidth: 360,
            detailWidth: 440,
            settleNanoseconds: 400_000_000
        )

        XCTAssertEqual((widerSidebar.contentMinX ?? 0) - (baseline.contentMinX ?? 0), 80, accuracy: 10)
        XCTAssertEqual(widerSidebar.detailMinX ?? 0, baseline.detailMinX ?? 0, accuracy: 10)
    }

    func testAppKitSplitViewRestoresPersistedDetailWidthOnLaunch() async throws {
        let pod = PodSummary(name: "sample-pod-7c9db", namespace: "team-alpha", status: "Running", totalRestarts: 1, ageDescription: "5m")

        let baseline = try await hostSnapshot(
            viewModel: makePodViewModel(pod: pod),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview,
                    shellVariant: .appKitSplitView,
                    manifestInlineEditorImplementation: .swiftUITextEditor
                )
            },
            section: .workloads,
            kind: .pod,
            sidebarWidth: 280,
            detailWidth: 440,
            settleNanoseconds: 400_000_000
        )

        let widerDetail = try await hostSnapshot(
            viewModel: makePodViewModel(pod: pod),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview,
                    shellVariant: .appKitSplitView,
                    manifestInlineEditorImplementation: .swiftUITextEditor
                )
            },
            section: .workloads,
            kind: .pod,
            sidebarWidth: 280,
            detailWidth: 520,
            settleNanoseconds: 400_000_000
        )

        XCTAssertEqual(widerDetail.contentMinX ?? 0, baseline.contentMinX ?? 0, accuracy: 10)
        XCTAssertEqual((widerDetail.detailMinX ?? 0) - (baseline.detailMinX ?? 0), -80, accuracy: 10)
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

    private func assertStableTransition(
        in window: NSWindow,
        snapshots: @escaping () -> [RuneRootLayoutSnapshot],
        viewModel: RuneAppViewModel,
        section: RuneSection,
        kind: KubeResourceKind
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
        let timeout = Date().addingTimeInterval(0.35)

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
                XCTAssertEqual(snapshot.contentMinY ?? 0, baselineContentY, accuracy: 2.0)
                XCTAssertEqual(snapshot.detailMinY ?? 0, baselineDetailY, accuracy: 2.0)
                XCTAssertEqual(snapshot.contentMinX ?? 0, baselineContentX, accuracy: horizontalOffsetAccuracy, "post-settle content MinX offset")
                XCTAssertEqual(snapshot.detailMinX ?? 0, baselineDetailX, accuracy: horizontalOffsetAccuracy, "post-settle detail MinX offset")
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

    private func assertAligned(baseline: RuneRootLayoutSnapshot, candidate: RuneRootLayoutSnapshot) {
        XCTAssertEqual(
            baseline.resolvedWindowTopInset,
            candidate.resolvedWindowTopInset,
            accuracy: 0.5
        )
        XCTAssertEqual(baseline.contentMinY ?? 0, candidate.contentMinY ?? 0, accuracy: verticalAlignmentAccuracy, "content top (minY) drift")
        XCTAssertEqual(baseline.headerMinY ?? 0, candidate.headerMinY ?? 0, accuracy: verticalAlignmentAccuracy, "header top (minY) drift")
        XCTAssertEqual(baseline.detailMinY ?? 0, candidate.detailMinY ?? 0, accuracy: verticalAlignmentAccuracy, "detail top (minY) drift")
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
}
