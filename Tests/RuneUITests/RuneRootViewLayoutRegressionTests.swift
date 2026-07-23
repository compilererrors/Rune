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

    func testRootContentDoesNotJumpWhenWindowChromeInsetArrives() async throws {
        let pod = PodSummary(
            name: "demo-api-546d7f8768-rzv22",
            namespace: "demo-namespace",
            status: "Running",
            totalRestarts: 0,
            ageDescription: "6d"
        )
        let viewModel = makePodViewModel(pod: pod)
        let initial = try await hostAppKitSplitSnapshot(
            viewModel: viewModel,
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview,
                    shellVariant: .appKitSplitView,
                    initialSidebarWidthOverride: 280,
                    initialDetailWidthOverride: 520
                )
            },
            section: .workloads,
            kind: .pod,
            settleNanoseconds: 300_000_000
        )
        let settled = try await hostAppKitSplitSnapshot(
            viewModel: viewModel,
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview,
                    shellVariant: .appKitSplitView,
                    initialSidebarWidthOverride: 280,
                    initialDetailWidthOverride: 520
                )
            },
            section: .workloads,
            kind: .pod,
            settleNanoseconds: 1_200_000_000
        )

        assertAppKitSplitUsesFullHeight(initial)
        assertAppKitSplitUsesFullHeight(settled)
        XCTAssertEqual(settled.splitViewMinY, initial.splitViewMinY, accuracy: 1.0)
        XCTAssertEqual(settled.splitViewHeight, initial.splitViewHeight, accuracy: 1.5)
    }

    func testAppKitSplitUsesFullUsableWindowHeightWithoutFooterGap() async throws {
        let pod = PodSummary(
            name: "demo-api-546d7f8768-rzv22",
            namespace: "demo-namespace",
            status: "Running",
            totalRestarts: 0,
            ageDescription: "6d"
        )
        let snapshot = try await hostAppKitSplitSnapshot(
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
            snapshot.splitViewMinY,
            0,
            accuracy: 1.0,
            "The root split should sit directly on the bottom edge so the sidebar footer, terminal, and detail shell are not clipped."
        )
        XCTAssertEqual(
            snapshot.splitViewHeight,
            snapshot.windowContentHeight,
            accuracy: 1.5,
            "The root split should fill the content view. The toolbar is outside the content area, and footer/status UI must be a real component rather than an invisible inset."
        )
    }

    func testAppKitSplitUsesFullUsableHeightOnCompactWindows() async throws {
        let pod = PodSummary(
            name: "demo-api-546d7f8768-rzv22",
            namespace: "demo-namespace",
            status: "Running",
            totalRestarts: 0,
            ageDescription: "6d"
        )
        let snapshot = try await hostAppKitSplitSnapshot(
            viewModel: makePodViewModel(pod: pod),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview,
                    shellVariant: .appKitSplitView,
                    manifestInlineEditorImplementation: .swiftUITextEditor,
                    initialSidebarWidthOverride: 240,
                    initialDetailWidthOverride: 320
                )
            },
            section: .workloads,
            kind: .pod,
            windowSize: CGSize(width: 980, height: 640),
            settleNanoseconds: 400_000_000
        )

        XCTAssertEqual(snapshot.splitViewMinY, 0, accuracy: 1.0)
        XCTAssertEqual(
            snapshot.splitViewHeight,
            snapshot.windowContentHeight,
            accuracy: 1.5,
            "Compact windows should not lose bottom content to fixed top or footer reservations."
        )
        XCTAssertEqual(
            snapshot.detailWidth,
            RuneInspectorScaffoldMetrics.supportedMinimumWidth,
            accuracy: 1.5,
            "The real Root shell must preserve the supported 320-point inspector width at 980×640."
        )
        XCTAssertEqual(snapshot.detailHeight, snapshot.splitViewHeight, accuracy: 1.5)
        XCTAssertGreaterThanOrEqual(snapshot.contentWidth, 360)
        XCTAssertGreaterThanOrEqual(snapshot.contentMinX, snapshot.sidebarWidth)
        XCTAssertGreaterThanOrEqual(snapshot.detailMinX, snapshot.contentMinX + snapshot.contentWidth)
        XCTAssertLessThanOrEqual(snapshot.detailMinX + snapshot.detailWidth, snapshot.splitViewWidth + 1)
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
            namespace: "example-namespace",
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

    func testRBACYAMLAndDescribeRemainTopAligned() async throws {
        let resource = ClusterResourceSummary(
            kind: .clusterRole,
            name: "demo-cluster-editor",
            namespace: nil,
            primaryText: "2 rules",
            secondaryText: "Cluster role"
        )
        for shellVariant in RuneRootShellVariant.allCases {
            for editorImplementation in ManifestInlineEditorImplementation.allCases {
                let yaml = try await hostSnapshot(
                    viewModel: makeRBACViewModel(resource: resource),
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
                    section: .rbac,
                    kind: .clusterRole,
                    settleNanoseconds: 250_000_000
                )
                let describe = try await hostSnapshot(
                    viewModel: makeRBACViewModel(resource: resource),
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
                    section: .rbac,
                    kind: .clusterRole,
                    settleNanoseconds: 250_000_000
                )

                assertAligned(baseline: yaml, candidate: describe)
            }
        }
    }

    func testWorkloadDescribeTextViewportIsBoundedInsideDetailColumn() async throws {
        let pod = PodSummary(
            name: "demo-api-546d7f8768-rzv22",
            namespace: "demo-namespace",
            status: "Running",
            totalRestarts: 0,
            ageDescription: "6d"
        )
        let metrics = try await hostManifestTextViewportMetrics(
            viewModel: makePodViewModel(pod: pod),
            initialPodInspectorTab: .describe
        )

        XCTAssertGreaterThan(metrics.textViewport.height, 120)
        XCTAssertGreaterThanOrEqual(metrics.textViewport.minX, metrics.detailBounds.minX - 1, "Describe text viewport should not render left of the detail column")
        XCTAssertGreaterThanOrEqual(metrics.textViewport.minY, metrics.detailBounds.minY - 1, "Describe text viewport should not render below the detail column")
        XCTAssertLessThanOrEqual(metrics.textViewport.maxX, metrics.detailBounds.maxX + 1, "Describe text viewport should not render right of the detail column")
        XCTAssertLessThanOrEqual(metrics.textViewport.maxY, metrics.detailBounds.maxY + 1, "Describe text viewport should not render above the detail column")
    }

    func testWorkloadYAMLAndDescribeUseBoundedTextViewportModel() async throws {
        let pod = PodSummary(
            name: "demo-api-546d7f8768-rzv22",
            namespace: "demo-namespace",
            status: "Running",
            totalRestarts: 0,
            ageDescription: "6d"
        )

        let yaml = try await hostManifestTextViewportMetrics(
            viewModel: makePodViewModel(pod: pod),
            initialPodInspectorTab: .yaml
        )
        let describe = try await hostManifestTextViewportMetrics(
            viewModel: makePodViewModel(pod: pod),
            initialPodInspectorTab: .describe
        )

        XCTAssertLessThanOrEqual(yaml.textViewport.maxY, yaml.detailBounds.maxY + 1, "YAML text viewport should be bounded by the detail column")
        XCTAssertLessThanOrEqual(describe.textViewport.maxY, describe.detailBounds.maxY + 1, "Describe text viewport should be bounded by the detail column")
        XCTAssertEqual(yaml.textViewport.minX, describe.textViewport.minX, accuracy: 2.0, "YAML and Describe should share the same text surface leading edge")
        XCTAssertEqual(yaml.textViewport.width, describe.textViewport.width, accuracy: 2.0, "YAML and Describe should share the same text surface width")
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

    func testAppKitSplitUserResizePersistsDividerWidthsWhenEnabled() async throws {
        let defaults = UserDefaults.standard
        let sidebarKey = RuneSettingsKeys.layoutSidebarWidth
        let detailKey = RuneSettingsKeys.layoutDetailWidth
        let originalSidebar = defaults.object(forKey: sidebarKey)
        let originalDetail = defaults.object(forKey: detailKey)
        defaults.set(280.0, forKey: sidebarKey)
        defaults.set(440.0, forKey: detailKey)
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

        let pod = PodSummary(
            name: "sample-pod-7c9db",
            namespace: "team-alpha",
            status: "Running",
            totalRestarts: 1,
            ageDescription: "5m"
        )
        let host = NSHostingController(
            rootView: RuneRootView(
                viewModel: makePodViewModel(pod: pod),
                onLayoutSnapshotChange: nil,
                debugDisableBootstrap: true,
                debugDisableLayoutPersistence: false,
                initialPodInspectorTab: .overview,
                shellVariant: .appKitSplitView,
                initialSidebarWidthOverride: 280,
                initialDetailWidthOverride: 440
            )
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
        hostView.translatesAutoresizingMaskIntoConstraints = false
        container.addSubview(hostView)
        NSLayoutConstraint.activate([
            hostView.leadingAnchor.constraint(equalTo: container.leadingAnchor),
            hostView.trailingAnchor.constraint(equalTo: container.trailingAnchor),
            hostView.topAnchor.constraint(equalTo: container.topAnchor),
            hostView.bottomAnchor.constraint(equalTo: container.bottomAnchor)
        ])
        window.makeKeyAndOrderFront(nil)
        defer {
            hostView.removeFromSuperview()
            window.orderOut(nil)
        }

        try await Task.sleep(nanoseconds: 500_000_000)
        container.layoutSubtreeIfNeeded()
        let splitView = try XCTUnwrap(firstThreePaneVerticalSplitView(in: container))
        let targetSidebarWidth: CGFloat = 340
        let targetDetailWidth: CGFloat = 500
        splitView.setPosition(targetSidebarWidth, ofDividerAt: 0)
        splitView.setPosition(
            splitView.bounds.width - splitView.dividerThickness - targetDetailWidth,
            ofDividerAt: 1
        )
        splitView.adjustSubviews()
        try await Task.sleep(nanoseconds: 350_000_000)

        XCTAssertEqual(defaults.double(forKey: sidebarKey), targetSidebarWidth, accuracy: 3)
        XCTAssertEqual(defaults.double(forKey: detailKey), targetDetailWidth, accuracy: 3)
    }

    func testConfigAndWorkloadsRemainTopAligned() async throws {
        for shellVariant in RuneRootShellVariant.allCases {
            let viewModel = RuneAppViewModel()
            prepareLayoutTestViewModel(viewModel)

            if shellVariant == .appKitSplitView {
                let workloadsSnapshot = try await hostAppKitSplitSnapshot(
                    viewModel: viewModel,
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            shellVariant: shellVariant
                        )
                    },
                    section: .workloads,
                    kind: .pod,
                    settleNanoseconds: 350_000_000
                )

                viewModel.state.selectedSection = .config
                viewModel.state.selectedWorkloadKind = .configMap
                let configSnapshot = try await hostAppKitSplitSnapshot(
                    viewModel: viewModel,
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            shellVariant: shellVariant
                        )
                    },
                    section: .config,
                    kind: .configMap,
                    settleNanoseconds: 350_000_000
                )

                assertAppKitSplitUsesFullHeight(workloadsSnapshot)
                assertAppKitSplitUsesFullHeight(configSnapshot)
                XCTAssertEqual(workloadsSnapshot.splitViewMinY, configSnapshot.splitViewMinY, accuracy: 1.0)
                XCTAssertEqual(workloadsSnapshot.splitViewHeight, configSnapshot.splitViewHeight, accuracy: 1.5)
                continue
            }

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

    func testMiddlePanelResourceSurfacesUseSharedOuterInset() async throws {
        let pod = PodSummary(
            name: "sample-pod",
            namespace: "sample-namespace",
            status: "Running",
            totalRestarts: 0,
            ageDescription: "5m"
        )
        let configMap = ClusterResourceSummary(
            kind: .configMap,
            name: "sample-config",
            namespace: "sample-namespace",
            primaryText: "2 keys",
            secondaryText: "5m"
        )
        let event = EventSummary(
            type: "Normal",
            reason: "Scheduled",
            objectName: "sample-pod",
            message: "Resource scheduled",
            lastTimestamp: "2026-01-01T00:00:00Z",
            involvedKind: "Pod",
            involvedNamespace: "sample-namespace"
        )
        let eventState = RuneAppState()
        eventState.selectedSection = .events
        eventState.selectedWorkloadKind = .event
        eventState.selectedNamespace = "sample-namespace"
        eventState.setEvents([event])

        let helmState = RuneAppState()
        helmState.selectedSection = .helm
        helmState.selectedNamespace = "sample-namespace"
        helmState.setHelmReleases([
            HelmReleaseSummary(
                name: "sample-release",
                namespace: "sample-namespace",
                revision: 1,
                updated: "2026-01-01 00:00:00",
                status: "deployed",
                chart: "sample-chart-1.0.0",
                appVersion: "1.0.0"
            )
        ])

        let scenarios: [(String, RuneAppViewModel, RuneSection, KubeResourceKind)] = [
            ("Pods", makePodViewModel(pod: pod), .workloads, .pod),
            ("ConfigMaps", makeConfigViewModel(resource: configMap), .config, .configMap),
            ("Events", RuneAppViewModel(state: eventState), .events, .event),
            ("Helm", RuneAppViewModel(state: helmState), .helm, .pod)
        ]

        for (label, viewModel, section, kind) in scenarios {
            let snapshot = try await hostMiddlePanelTableSnapshot(
                viewModel: viewModel,
                section: section,
                kind: kind
            )
            XCTAssertEqual(
                snapshot.tableFrame.minX,
                RuneUILayoutMetrics.paneOuterPadding,
                accuracy: 0.75,
                "\(label) should start on the shared middle-panel leading grid."
            )
            XCTAssertEqual(
                snapshot.contentWidth - snapshot.tableFrame.maxX,
                RuneUILayoutMetrics.paneOuterPadding,
                accuracy: 0.75,
                "\(label) should end on the shared middle-panel trailing grid."
            )
        }
    }

    func testHelmReleaseAndOperatorTablesShareTheSameToolbarGrid() async throws {
        let releaseState = RuneAppState()
        releaseState.selectedSection = .helm
        releaseState.selectedNamespace = "sample-namespace"
        releaseState.setHelmReleases([
            HelmReleaseSummary(
                name: "sample-release",
                namespace: "sample-namespace",
                revision: 1,
                updated: "2026-01-01 00:00:00",
                status: "deployed",
                chart: "sample-chart-1.0.0",
                appVersion: "1.0.0"
            )
        ])

        let operatorResource = OperatorResourceSummary(
            family: "Example operator",
            kind: "Applications",
            apiPath: "/apis/example.io/v1/namespaces/sample-namespace/applications",
            name: "sample-application",
            namespace: "sample-namespace",
            status: "Ready True",
            message: "Reconciled",
            printerColumns: [
                OperatorResourceSummary.PrinterColumn(title: "Ready", value: "True")
            ]
        )
        let operatorState = RuneAppState()
        operatorState.selectedSection = .helm
        operatorState.selectedNamespace = "sample-namespace"
        operatorState.setOperatorResources([operatorResource])
        operatorState.setSelectedOperatorResource(operatorResource)

        for windowWidth in [CGFloat(1_440), CGFloat(1_160)] {
            let releaseSnapshot = try await hostMiddlePanelTableSnapshot(
                viewModel: RuneAppViewModel(state: releaseState),
                section: .helm,
                kind: .pod,
                windowWidth: windowWidth
            )
            let operatorSnapshot = try await hostMiddlePanelTableSnapshot(
                viewModel: RuneAppViewModel(state: operatorState),
                section: .helm,
                kind: .pod,
                windowWidth: windowWidth
            )

            XCTAssertEqual(
                operatorSnapshot.tableFrame.maxY,
                releaseSnapshot.tableFrame.maxY,
                accuracy: 1,
                "Helm releases and operator resources should enter the table on the same shared grid at width \(windowWidth)."
            )
        }
    }

    func testResourceControlGridFramesAlignAtDefaultAndCompactWidths() async throws {
        let pod = PodSummary(
            name: "sample-pod",
            namespace: "sample-namespace",
            status: "Running",
            totalRestarts: 0,
            ageDescription: "5m"
        )
        let configMap = ClusterResourceSummary(
            kind: .configMap,
            name: "sample-config",
            namespace: "sample-namespace",
            primaryText: "2 keys",
            secondaryText: "5m"
        )
        let scenarios: [(String, RuneSection, KubeResourceKind, () -> RuneAppViewModel)] = [
            ("Pods", .workloads, .pod, { self.makePodViewModel(pod: pod) }),
            ("ConfigMaps", .config, .configMap, { self.makeConfigViewModel(resource: configMap) })
        ]

        for (width, expectsCompactToolbar) in [(CGFloat(1_600), false), (CGFloat(1_160), true)] {
            for (label, section, kind, makeViewModel) in scenarios {
                let snapshot = try await hostMiddlePanelTableSnapshot(
                    viewModel: makeViewModel(),
                    section: section,
                    kind: kind,
                    windowWidth: width
                )
                assertResourceControlGrid(
                    snapshot,
                    label: "\(label) at \(Int(width))pt",
                    expectsCompactToolbar: expectsCompactToolbar
                )
            }
        }
    }

    func testWorkloadResourceToolbarKeepsTableTopStableAcrossKindsAndSelectionState() async throws {
        let pod = PodSummary(
            name: "sample-pod",
            namespace: "sample-namespace",
            status: "Running",
            totalRestarts: 0,
            ageDescription: "5m"
        )
        let deployment = DeploymentSummary(
            name: "sample-deployment",
            namespace: "sample-namespace",
            readyReplicas: 1,
            desiredReplicas: 1
        )
        let statefulSet = ClusterResourceSummary(
            kind: .statefulSet,
            name: "sample-statefulset",
            namespace: "sample-namespace",
            primaryText: "1/1 ready",
            secondaryText: "5m"
        )

        let podViewModel = makePodViewModel(pod: pod)
        let podSnapshot = try await hostMiddlePanelTableSnapshot(
            viewModel: podViewModel,
            section: .workloads,
            kind: .pod
        )
        podViewModel.togglePodBulkSelection(pod)
        let selectedPodSnapshot = try await hostMiddlePanelTableSnapshot(
            viewModel: podViewModel,
            section: .workloads,
            kind: .pod
        )
        let deploymentSnapshot = try await hostMiddlePanelTableSnapshot(
            viewModel: makeDeploymentViewModel(deployment: deployment),
            section: .workloads,
            kind: .deployment
        )

        let statefulSetState = RuneAppState()
        statefulSetState.selectedSection = .workloads
        statefulSetState.selectedWorkloadKind = .statefulSet
        statefulSetState.selectedNamespace = "sample-namespace"
        statefulSetState.setStatefulSets([statefulSet])
        statefulSetState.setSelectedStatefulSet(statefulSet)
        let statefulSetSnapshot = try await hostMiddlePanelTableSnapshot(
            viewModel: RuneAppViewModel(state: statefulSetState),
            section: .workloads,
            kind: .statefulSet
        )

        for (label, snapshot) in [
            ("selected Pods", selectedPodSnapshot),
            ("Deployments", deploymentSnapshot),
            ("StatefulSets", statefulSetSnapshot)
        ] {
            XCTAssertEqual(
                snapshot.tableFrame.maxY,
                podSnapshot.tableFrame.maxY,
                accuracy: 1.0,
                "\(label) should share the same table top edge as unselected Pods."
            )
        }

        podViewModel.clearPodBulkSelection()
        let compactPodSnapshot = try await hostMiddlePanelTableSnapshot(
            viewModel: podViewModel,
            section: .workloads,
            kind: .pod,
            windowWidth: 1_160
        )
        podViewModel.togglePodBulkSelection(pod)
        let compactSelectedPodSnapshot = try await hostMiddlePanelTableSnapshot(
            viewModel: podViewModel,
            section: .workloads,
            kind: .pod,
            windowWidth: 1_160
        )
        let compactDeploymentSnapshot = try await hostMiddlePanelTableSnapshot(
            viewModel: makeDeploymentViewModel(deployment: deployment),
            section: .workloads,
            kind: .deployment,
            windowWidth: 1_160
        )

        XCTAssertEqual(
            compactSelectedPodSnapshot.tableFrame.maxY,
            compactPodSnapshot.tableFrame.maxY,
            accuracy: 1,
            "Compact selection actions must not move the table top edge."
        )
        XCTAssertEqual(
            compactDeploymentSnapshot.tableFrame.maxY,
            compactPodSnapshot.tableFrame.maxY,
            accuracy: 1,
            "Compact resource toolbars must reserve the same two-row grid."
        )
    }

    func testSectionTransitionsDoNotDriftAfterLayoutSettles() async throws {
        for shellVariant in RuneRootShellVariant.allCases {
            let viewModel = RuneAppViewModel()
            prepareLayoutTestViewModel(viewModel)
            if shellVariant == .appKitSplitView {
                var previousSnapshot: AppKitSplitLaunchSnapshot?
                for transition in [
                    (RuneSection.workloads, KubeResourceKind.pod),
                    (.config, .configMap),
                    (.rbac, .role),
                    (.workloads, .pod)
                ] {
                    viewModel.state.selectedSection = transition.0
                    viewModel.state.selectedWorkloadKind = transition.1
                    let snapshot = try await hostAppKitSplitSnapshot(
                        viewModel: viewModel,
                        rootView: { viewModel, capture in
                            RuneRootView(
                                viewModel: viewModel,
                                onLayoutSnapshotChange: capture,
                                debugDisableBootstrap: true,
                                shellVariant: shellVariant
                            )
                        },
                        section: transition.0,
                        kind: transition.1,
                        settleNanoseconds: 350_000_000
                    )
                    assertAppKitSplitUsesFullHeight(snapshot)
                    if let previousSnapshot {
                        XCTAssertEqual(snapshot.splitViewMinY, previousSnapshot.splitViewMinY, accuracy: 1.0)
                        XCTAssertEqual(snapshot.splitViewHeight, previousSnapshot.splitViewHeight, accuracy: 1.5)
                    }
                    previousSnapshot = snapshot
                }
                continue
            }

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
            if shellVariant == .appKitSplitView {
                let initial = try await hostAppKitSplitSnapshot(
                    viewModel: viewModel,
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialPodInspectorTab: .overview,
                            shellVariant: shellVariant
                        )
                    },
                    section: .workloads,
                    kind: .pod,
                    settleNanoseconds: 350_000_000
                )
                let settled = try await hostAppKitSplitSnapshot(
                    viewModel: viewModel,
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialPodInspectorTab: .overview,
                            shellVariant: shellVariant
                        )
                    },
                    section: .workloads,
                    kind: .pod,
                    settleNanoseconds: UInt64(Self.longPodPostSettleObservationDuration * 1_000_000_000)
                )
                assertAppKitSplitUsesFullHeight(initial)
                assertAppKitSplitUsesFullHeight(settled)
                XCTAssertEqual(settled.splitViewMinY, initial.splitViewMinY, accuracy: 1.0)
                XCTAssertEqual(settled.splitViewHeight, initial.splitViewHeight, accuracy: 1.5)
                continue
            }

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
            nodeName: "demo-node-001",
            qosClass: "Burstable",
            containersReady: "2/2",
            containerNamesLine: "web, sidecar"
        )
        let finishLoading: (RuneAppState) -> Void = { state in
            state.setPods(self.makePodLoadingRows(seed: basePod, count: 36))
            state.isLoading = false
            state.beginResourceDetailLoad()
            state.setSelectedPod(basePod.mergingInspectorDetail(enrichedPod))
            state.setResourceYAML(self.sampleYAML(named: basePod.name))
            state.setResourceDescribe(self.sampleDescribe(named: basePod.name))
            state.finishResourceDetailLoad()
        }

        for shellVariant in RuneRootShellVariant.allCases {
            let state = RuneAppState()
            state.selectedSection = .workloads
            state.selectedWorkloadKind = .pod
            state.selectedNamespace = basePod.namespace
            state.isLoading = true

            let viewModel = RuneAppViewModel(state: state)
            prepareLayoutTestViewModel(viewModel)

            if shellVariant == .appKitSplitView {
                let loadingSnapshot = try await hostAppKitSplitSnapshot(
                    viewModel: viewModel,
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialPodInspectorTab: .overview,
                            shellVariant: shellVariant
                        )
                    },
                    section: .workloads,
                    kind: .pod,
                    settleNanoseconds: 350_000_000
                )

                finishLoading(state)

                let loadedSnapshot = try await hostAppKitSplitSnapshot(
                    viewModel: viewModel,
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialPodInspectorTab: .overview,
                            shellVariant: shellVariant
                        )
                    },
                    section: .workloads,
                    kind: .pod,
                    settleNanoseconds: 650_000_000
                )

                assertAppKitSplitUsesFullHeight(loadingSnapshot)
                assertAppKitSplitUsesFullHeight(loadedSnapshot)
                XCTAssertEqual(loadedSnapshot.contentMinY, loadingSnapshot.contentMinY, accuracy: 2.0)
                XCTAssertEqual(loadedSnapshot.detailMinY, loadingSnapshot.detailMinY, accuracy: 2.0)
                XCTAssertEqual(loadedSnapshot.contentMinX, loadingSnapshot.contentMinX, accuracy: horizontalOffsetAccuracy)
                XCTAssertEqual(loadedSnapshot.detailMinX, loadingSnapshot.detailMinX, accuracy: horizontalOffsetAccuracy)
                XCTAssertEqual(loadedSnapshot.contentHeight, loadingSnapshot.contentHeight, accuracy: 1.5)
                XCTAssertEqual(loadedSnapshot.detailHeight, loadingSnapshot.detailHeight, accuracy: 1.5)
                continue
            }

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

            let loadingSnapshot: RuneRootLayoutSnapshot
            do {
                loadingSnapshot = try await stableSnapshot(
                    in: window,
                    snapshots: { snapshots },
                    section: .workloads,
                    kind: .pod,
                    observationDuration: 0.4
                )
            } catch {
                XCTFail("Initial loading layout did not settle for shell=\(shellVariant.debugLabel): \(error)")
                throw error
            }

            finishLoading(state)

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

    func testAppKitSplitWidthsDoNotJumpAfterProgrammaticHydration() async throws {
        let basePod = PodSummary(
            name: "demo-api-546d7f8768-rzv22",
            namespace: "demo-namespace",
            status: "Running",
            totalRestarts: 0,
            ageDescription: "6d"
        )
        let enrichedPod = PodSummary(
            name: basePod.name,
            namespace: basePod.namespace,
            status: basePod.status,
            totalRestarts: basePod.totalRestarts,
            ageDescription: basePod.ageDescription,
            cpuUsage: "3m",
            memoryUsage: "400Mi",
            podIP: "10.244.4.11",
            hostIP: "10.224.0.7",
            nodeName: "demo-node-001",
            qosClass: "Burstable",
            containersReady: "1/1",
            containerNamesLine: "demo-api"
        )
        let state = RuneAppState()
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.selectedNamespace = basePod.namespace
        state.isLoading = true

        let viewModel = RuneAppViewModel(state: state)
        prepareLayoutTestViewModel(viewModel)
        let host = NSHostingController(
            rootView: RuneRootView(
                viewModel: viewModel,
                onLayoutSnapshotChange: nil,
                debugDisableBootstrap: true,
                initialPodInspectorTab: .overview,
                shellVariant: .appKitSplitView,
                initialSidebarWidthOverride: 280,
                initialDetailWidthOverride: 520
            )
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        let container = NSView(frame: NSRect(x: 0, y: 0, width: 1440, height: 900))
        window.contentView = container
        let hostView: NSView = host.view
        hostView.frame = container.bounds
        hostView.autoresizingMask = [.width, .height]
        container.addSubview(hostView)
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        let loading = try await waitForAppKitSplitSnapshot(in: container)

        state.setPods(makePodLoadingRows(seed: basePod, count: 120))
        state.isLoading = false
        state.beginResourceDetailLoad()
        state.setSelectedPod(basePod.mergingInspectorDetail(enrichedPod))
        state.setResourceYAML(sampleYAML(named: basePod.name))
        state.setResourceDescribe(sampleDescribe(named: basePod.name))
        state.finishResourceDetailLoad()

        try await Task.sleep(nanoseconds: 1_000_000_000)
        let hydrated = try await waitForAppKitSplitSnapshot(in: container)

        XCTAssertEqual(hydrated.sidebarWidth, loading.sidebarWidth, accuracy: 1.5, "sidebar width jumped after programmatic hydration")
        XCTAssertEqual(hydrated.detailWidth, loading.detailWidth, accuracy: 1.5, "detail width jumped after programmatic hydration")
        XCTAssertEqual(hydrated.contentMinX, loading.contentMinX, accuracy: 1.5, "content divider jumped after programmatic hydration")
        XCTAssertEqual(hydrated.detailMinX, loading.detailMinX, accuracy: 1.5, "detail divider jumped after programmatic hydration")
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
            prepareLayoutTestViewModel(viewModel)
            if shellVariant == .appKitSplitView {
                let baseline = try await hostAppKitSplitSnapshot(
                    viewModel: viewModel,
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            shellVariant: shellVariant
                        )
                    },
                    section: .events,
                    kind: .event,
                    settleNanoseconds: 350_000_000
                )

                viewModel.openEventSource(event)
                let stillEvents = try await hostAppKitSplitSnapshot(
                    viewModel: viewModel,
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            shellVariant: shellVariant
                        )
                    },
                    section: .events,
                    kind: .event,
                    settleNanoseconds: 350_000_000
                )
                assertAppKitSplitUsesFullHeight(baseline)
                assertAppKitSplitUsesFullHeight(stillEvents)
                XCTAssertEqual(stillEvents.splitViewMinY, baseline.splitViewMinY, accuracy: 1.0)
                XCTAssertEqual(stillEvents.splitViewHeight, baseline.splitViewHeight, accuracy: 1.5)

                state.setHorizontalPodAutoscalers([autoscaler])
                viewModel.openEventSource(event)
                let navigated = try await hostAppKitSplitSnapshot(
                    viewModel: viewModel,
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            shellVariant: shellVariant
                        )
                    },
                    section: .workloads,
                    kind: .horizontalPodAutoscaler,
                    settleNanoseconds: 350_000_000
                )
                assertAppKitSplitUsesFullHeight(navigated)
                XCTAssertEqual(navigated.splitViewMinY, baseline.splitViewMinY, accuracy: 1.0)
                XCTAssertEqual(navigated.splitViewHeight, baseline.splitViewHeight, accuracy: 1.5)
                XCTAssertEqual(viewModel.state.selectedHorizontalPodAutoscaler?.name, autoscaler.name)
                continue
            }

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
            prepareLayoutTestViewModel(viewModel)
            if shellVariant == .appKitSplitView {
                let eventsSnapshot = try await hostAppKitSplitSnapshot(
                    viewModel: viewModel,
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialPodInspectorTab: .overview,
                            shellVariant: shellVariant
                        )
                    },
                    section: .events,
                    kind: .event,
                    settleNanoseconds: 350_000_000
                )

                state.selectedSection = .workloads
                state.selectedWorkloadKind = .pod
                state.isLoading = true
                let loadingSnapshot = try await hostAppKitSplitSnapshot(
                    viewModel: viewModel,
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialPodInspectorTab: .overview,
                            shellVariant: shellVariant
                        )
                    },
                    section: .workloads,
                    kind: .pod,
                    settleNanoseconds: 350_000_000
                )

                let firstWave = makePodLoadingRows(seed: loadingSeed, count: 120)
                state.setPods(firstWave)
                state.setSelectedPod(firstWave.first(where: { $0.name == targetPodName }))
                state.beginResourceDetailLoad()
                state.setResourceYAML(sampleYAML(named: targetPodName))
                state.setResourceDescribe(sampleDescribe(named: targetPodName))
                state.finishResourceDetailLoad()
                state.isLoading = false

                let hydratedSnapshot = try await hostAppKitSplitSnapshot(
                    viewModel: viewModel,
                    rootView: { viewModel, capture in
                        RuneRootView(
                            viewModel: viewModel,
                            onLayoutSnapshotChange: capture,
                            debugDisableBootstrap: true,
                            initialPodInspectorTab: .overview,
                            shellVariant: shellVariant
                        )
                    },
                    section: .workloads,
                    kind: .pod,
                    settleNanoseconds: 650_000_000
                )

                assertAppKitSplitUsesFullHeight(eventsSnapshot)
                assertAppKitSplitUsesFullHeight(loadingSnapshot)
                assertAppKitSplitUsesFullHeight(hydratedSnapshot)
                XCTAssertEqual(loadingSnapshot.splitViewMinY, eventsSnapshot.splitViewMinY, accuracy: 1.0)
                XCTAssertEqual(hydratedSnapshot.splitViewMinY, loadingSnapshot.splitViewMinY, accuracy: 1.0)
                XCTAssertEqual(loadingSnapshot.splitViewHeight, eventsSnapshot.splitViewHeight, accuracy: 1.5)
                XCTAssertEqual(hydratedSnapshot.splitViewHeight, loadingSnapshot.splitViewHeight, accuracy: 1.5)
                continue
            }

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

        let capturedSnapshots = snapshots()
        let lastSnapshot = capturedSnapshots.last.map { "\($0)" } ?? "<none>"
        XCTFail("Timed out waiting for layout snapshot; captured=\(capturedSnapshots.count) last=\(lastSnapshot)")
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
        var nextSnapshotIndex = settledSnapshotCount

        while Date() < timeout {
            window.contentView?.layoutSubtreeIfNeeded()
            await Task.yield()

            let capturedSnapshots = snapshots()
            let recentSnapshots = capturedSnapshots.dropFirst(nextSnapshotIndex).filter {
                $0.section == section
                    && $0.workloadKind == kind
                    && $0.contentMinY != nil
                    && $0.headerMinY != nil
                    && $0.detailMinY != nil
                    && $0.contentMinX != nil
                    && $0.headerMinX != nil
                    && $0.detailMinX != nil
            }
            nextSnapshotIndex = capturedSnapshots.count

            for snapshot in recentSnapshots {
                XCTAssertEqual(
                    snapshot.contentMinY ?? 0,
                    baseline.contentMinY ?? 0,
                    accuracy: 2.0,
                    "post-settle content MinY drift; baseline=\(baseline) candidate=\(snapshot)"
                )
                XCTAssertEqual(
                    snapshot.detailMinY ?? 0,
                    baseline.detailMinY ?? 0,
                    accuracy: 2.0,
                    "post-settle detail MinY drift; baseline=\(baseline) candidate=\(snapshot)"
                )
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
        let viewModel = RuneAppViewModel(state: state)
        prepareLayoutTestViewModel(viewModel)
        return viewModel
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
        let viewModel = RuneAppViewModel(state: state)
        prepareLayoutTestViewModel(viewModel)
        return viewModel
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
        let viewModel = RuneAppViewModel(state: state)
        prepareLayoutTestViewModel(viewModel)
        return viewModel
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
        let viewModel = RuneAppViewModel(state: state)
        prepareLayoutTestViewModel(viewModel)
        return viewModel
    }

    private func makeRBACViewModel(resource: ClusterResourceSummary) -> RuneAppViewModel {
        let state = RuneAppState()
        state.selectedSection = .rbac
        state.selectedWorkloadKind = resource.kind
        state.selectedNamespace = resource.namespace ?? "default"
        state.setRBACData(
            roles: resource.kind == .role ? [resource] : [],
            serviceAccounts: resource.kind == .serviceAccount ? [resource] : [],
            roleBindings: resource.kind == .roleBinding ? [resource] : [],
            clusterRoles: resource.kind == .clusterRole ? [resource] : [],
            clusterRoleBindings: resource.kind == .clusterRoleBinding ? [resource] : []
        )
        state.setSelectedRBACResource(resource)
        state.setResourceYAML(sampleYAML(named: resource.name))
        state.setResourceDescribe(sampleDescribe(named: resource.name))
        let viewModel = RuneAppViewModel(state: state)
        prepareLayoutTestViewModel(viewModel)
        return viewModel
    }

    private func prepareLayoutTestViewModel(_ viewModel: RuneAppViewModel) {
        viewModel.isSidebarVisible = true
        viewModel.isDetailPaneVisible = true
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
        prepareLayoutTestViewModel(viewModel)
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

    private struct ManifestTextViewportMetrics {
        let detailBounds: CGRect
        let textViewport: CGRect
    }

    private func hostManifestTextViewportMetrics(
        viewModel: RuneAppViewModel,
        initialPodInspectorTab: PodInspectorTab,
        settleNanoseconds: UInt64 = 500_000_000
    ) async throws -> ManifestTextViewportMetrics {
        prepareLayoutTestViewModel(viewModel)
        let host = NSHostingController(
            rootView: RuneRootView(
                viewModel: viewModel,
                onLayoutSnapshotChange: nil,
                debugDisableBootstrap: true,
                initialPodInspectorTab: initialPodInspectorTab,
                shellVariant: .appKitSplitView,
                manifestInlineEditorImplementation: .appKitTextView,
                initialYAMLInlineEditing: false,
                initialSidebarWidthOverride: 280,
                initialDetailWidthOverride: 520
            )
        )

        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        let container = NSView(frame: NSRect(x: 0, y: 0, width: 1440, height: 900))
        window.contentView = container
        let hostView: NSView = host.view
        hostView.frame = container.bounds
        hostView.autoresizingMask = [.width, .height]
        container.addSubview(hostView)
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await Task.sleep(nanoseconds: settleNanoseconds)
        container.layoutSubtreeIfNeeded()
        try await Task.sleep(nanoseconds: 50_000_000)
        container.layoutSubtreeIfNeeded()

        guard let splitView = firstThreePaneVerticalSplitView(in: container),
              splitView.arrangedSubviews.count == 3
        else {
            XCTFail("Expected root AppKit split view")
            throw CancellationError()
        }

        let detailView = splitView.arrangedSubviews[2]
        guard let textScrollView = firstTextScrollView(in: detailView) else {
            XCTFail("Expected manifest NSTextView-backed scroll view in detail pane")
            throw CancellationError()
        }

        return ManifestTextViewportMetrics(
            detailBounds: detailView.bounds,
            textViewport: textScrollView.superview?.convert(textScrollView.frame, to: detailView)
                ?? textScrollView.convert(textScrollView.bounds, to: detailView)
        )
    }

    private struct AppKitSplitLaunchSnapshot {
        let splitViewWidth: CGFloat
        let windowContentWidth: CGFloat
        let splitViewMinY: CGFloat
        let splitViewHeight: CGFloat
        let windowContentHeight: CGFloat
        let sidebarMinY: CGFloat
        let sidebarHeight: CGFloat
        let contentMinY: CGFloat
        let contentHeight: CGFloat
        let detailMinY: CGFloat
        let detailHeight: CGFloat
        let sidebarWidth: CGFloat
        let contentMinX: CGFloat
        let contentWidth: CGFloat
        let detailMinX: CGFloat
        let detailWidth: CGFloat
    }

    private struct MiddlePanelTableSnapshot {
        let contentWidth: CGFloat
        let contentHeight: CGFloat
        let tableFrame: CGRect
        let layout: RuneRootLayoutSnapshot
    }

    private func hostMiddlePanelTableSnapshot(
        viewModel: RuneAppViewModel,
        section: RuneSection,
        kind: KubeResourceKind,
        windowWidth: CGFloat = 1_440
    ) async throws -> MiddlePanelTableSnapshot {
        prepareLayoutTestViewModel(viewModel)
        viewModel.state.selectedSection = section
        viewModel.state.selectedWorkloadKind = kind
        var layoutSnapshots: [RuneRootLayoutSnapshot] = []

        let host = NSHostingController(
            rootView: RuneRootView(
                viewModel: viewModel,
                onLayoutSnapshotChange: { snapshot in
                    layoutSnapshots.append(snapshot)
                },
                debugDisableBootstrap: true,
                shellVariant: .appKitSplitView,
                initialSidebarWidthOverride: 280,
                initialDetailWidthOverride: 520
            )
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: windowWidth, height: 900),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        let container = NSView(frame: NSRect(x: 0, y: 0, width: windowWidth, height: 900))
        window.contentView = container
        let hostView = host.view
        hostView.translatesAutoresizingMaskIntoConstraints = false
        container.addSubview(hostView)
        NSLayoutConstraint.activate([
            hostView.leadingAnchor.constraint(equalTo: container.leadingAnchor),
            hostView.trailingAnchor.constraint(equalTo: container.trailingAnchor),
            hostView.topAnchor.constraint(equalTo: container.topAnchor),
            hostView.bottomAnchor.constraint(equalTo: container.bottomAnchor)
        ])
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        let timeout = Date().addingTimeInterval(2.0)
        while Date() < timeout {
            container.layoutSubtreeIfNeeded()
            await Task.yield()

            if let splitView = firstThreePaneVerticalSplitView(in: container),
               splitView.arrangedSubviews.count == 3 {
                let contentView = splitView.arrangedSubviews[1]
                if let tableView = firstTableView(in: contentView),
                   let scrollView = tableView.enclosingScrollView,
                   scrollView.bounds.width > 1,
                   let layout = layoutSnapshots.last(where: {
                       $0.section == section
                           && $0.resourceFamilyFrame != nil
                           && $0.resourceToolbarFrame != nil
                           && $0.resourceFilterRailFrame != nil
                           && $0.resourceActionsRailFrame != nil
                           && $0.resourceTableSurfaceFrame != nil
                   }) {
                    return MiddlePanelTableSnapshot(
                        contentWidth: contentView.bounds.width,
                        contentHeight: contentView.bounds.height,
                        tableFrame: scrollView.convert(scrollView.bounds, to: contentView),
                        layout: layout
                    )
                }
            }

            try await Task.sleep(nanoseconds: 20_000_000)
        }

        XCTFail("Timed out waiting for the \(section.rawValue) middle-panel table surface")
        throw CancellationError()
    }

    private func assertResourceControlGrid(
        _ snapshot: MiddlePanelTableSnapshot,
        label: String,
        expectsCompactToolbar: Bool
    ) {
        guard let contentMinX = snapshot.layout.contentMinX,
              let contentMinY = snapshot.layout.contentMinY,
              let family = snapshot.layout.resourceFamilyFrame,
              let toolbar = snapshot.layout.resourceToolbarFrame,
              let filterRail = snapshot.layout.resourceFilterRailFrame,
              let actionsRail = snapshot.layout.resourceActionsRailFrame,
              let tableSurface = snapshot.layout.resourceTableSurfaceFrame else {
            XCTFail("\(label) did not report the complete hosted resource-control hierarchy.")
            return
        }

        for (name, frame) in [
            ("family", family),
            ("toolbar", toolbar),
            ("filter", filterRail),
            ("actions", actionsRail),
            ("table", tableSurface)
        ] {
            XCTAssertGreaterThan(frame.width, 1, "\(label) \(name) rail must have a real hosted width.")
            XCTAssertGreaterThan(frame.height, 1, "\(label) \(name) rail must have a real hosted height.")
        }

        XCTAssertEqual(family.minX, toolbar.minX, accuracy: 0.75, "\(label) family and toolbar leading edges drifted.")
        XCTAssertEqual(toolbar.minX, tableSurface.minX, accuracy: 0.75, "\(label) toolbar and table leading edges drifted.")
        XCTAssertEqual(family.width, toolbar.width, accuracy: 0.75, "\(label) family and toolbar widths should share the grid.")
        XCTAssertEqual(toolbar.width, tableSurface.width, accuracy: 0.75, "\(label) toolbar and table widths should share the grid.")
        XCTAssertEqual(
            toolbar.minY - family.maxY,
            RuneUILayoutMetrics.contentControlSpacing,
            accuracy: 0.75,
            "\(label) family-to-toolbar gap changed or overlapped."
        )
        XCTAssertEqual(
            tableSurface.minY - toolbar.maxY,
            RuneUILayoutMetrics.contentModuleSpacing,
            accuracy: 0.75,
            "\(label) toolbar-to-table gap changed or overlapped."
        )

        XCTAssertGreaterThanOrEqual(filterRail.minX, toolbar.minX - 0.5, "\(label) filter escaped the toolbar.")
        XCTAssertLessThanOrEqual(filterRail.maxX, toolbar.maxX + 0.5, "\(label) filter escaped the toolbar.")
        XCTAssertGreaterThanOrEqual(actionsRail.minX, toolbar.minX - 0.5, "\(label) actions escaped the toolbar.")
        XCTAssertLessThanOrEqual(actionsRail.maxX, toolbar.maxX + 0.5, "\(label) actions escaped the toolbar.")

        if expectsCompactToolbar {
            XCTAssertEqual(filterRail.minX, actionsRail.minX, accuracy: 0.75, "\(label) compact rails should share a leading edge.")
            XCTAssertEqual(filterRail.width, actionsRail.width, accuracy: 0.75, "\(label) compact rails should share a width.")
            XCTAssertEqual(
                actionsRail.minY - filterRail.maxY,
                RuneUILayoutMetrics.resourceListCompactRowSpacing,
                accuracy: 0.75,
                "\(label) compact rails should stack without overlap."
            )
        } else {
            XCTAssertEqual(filterRail.midY, actionsRail.midY, accuracy: 0.75, "\(label) default rails should share a baseline.")
            XCTAssertEqual(
                actionsRail.minX - filterRail.maxX,
                RuneUILayoutMetrics.contentControlSpacing,
                accuracy: 0.75,
                "\(label) default rails should use the shared horizontal gutter."
            )
        }

        XCTAssertEqual(
            family.minX - contentMinX,
            snapshot.tableFrame.minX,
            accuracy: 1,
            "\(label) control grid and native table should share the pane inset."
        )
        XCTAssertEqual(
            tableSurface.width,
            snapshot.tableFrame.width,
            accuracy: 1,
            "\(label) control grid and native table should share the available width."
        )
        XCTAssertEqual(
            tableSurface.minY - contentMinY,
            snapshot.contentHeight - snapshot.tableFrame.maxY,
            accuracy: 1.5,
            "\(label) the native table should start exactly at the probed table region."
        )
    }

    private func firstTableView(in view: NSView?) -> NSTableView? {
        guard let view else { return nil }
        if let tableView = view as? NSTableView {
            return tableView
        }
        for subview in view.subviews {
            if let tableView = firstTableView(in: subview) {
                return tableView
            }
        }
        return nil
    }

    private func hostAppKitSplitSnapshot(
        viewModel: RuneAppViewModel,
        rootView: (RuneAppViewModel, @escaping (RuneRootLayoutSnapshot) -> Void) -> RuneRootView,
        section: RuneSection,
        kind: KubeResourceKind,
        windowSize: CGSize = CGSize(width: 1440, height: 900),
        settleNanoseconds: UInt64 = 0
    ) async throws -> AppKitSplitLaunchSnapshot {
        prepareLayoutTestViewModel(viewModel)
        let host = NSHostingController(
            rootView: rootView(viewModel) { _ in }
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: windowSize.width, height: windowSize.height),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        let container = NSView(frame: NSRect(x: 0, y: 0, width: windowSize.width, height: windowSize.height))
        window.contentView = container
        let hostView = host.view
        hostView.translatesAutoresizingMaskIntoConstraints = false
        container.addSubview(hostView)
        NSLayoutConstraint.activate([
            hostView.leadingAnchor.constraint(equalTo: container.leadingAnchor),
            hostView.trailingAnchor.constraint(equalTo: container.trailingAnchor),
            hostView.topAnchor.constraint(equalTo: container.topAnchor),
            hostView.bottomAnchor.constraint(equalTo: container.bottomAnchor)
        ])
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
                        splitViewMinY: splitView.frame.minY,
                        splitViewHeight: splitView.frame.height,
                        windowContentHeight: rootView.bounds.height,
                        sidebarMinY: sidebar.minY,
                        sidebarHeight: sidebar.height,
                        contentMinY: content.minY,
                        contentHeight: content.height,
                        detailMinY: detail.minY,
                        detailHeight: detail.height,
                        sidebarWidth: sidebar.width,
                        contentMinX: content.minX,
                        contentWidth: content.width,
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

    private func assertAppKitSplitUsesFullHeight(_ snapshot: AppKitSplitLaunchSnapshot, file: StaticString = #filePath, line: UInt = #line) {
        XCTAssertEqual(snapshot.splitViewMinY, 0, accuracy: 1.0, file: file, line: line)
        XCTAssertEqual(snapshot.splitViewHeight, snapshot.windowContentHeight, accuracy: 1.5, file: file, line: line)
        XCTAssertEqual(snapshot.sidebarMinY, 0, accuracy: 1.0, file: file, line: line)
        XCTAssertEqual(snapshot.contentMinY, 0, accuracy: 1.0, file: file, line: line)
        XCTAssertEqual(snapshot.detailMinY, 0, accuracy: 1.0, file: file, line: line)
        XCTAssertEqual(snapshot.sidebarHeight, snapshot.splitViewHeight, accuracy: 1.5, file: file, line: line)
        XCTAssertEqual(snapshot.contentHeight, snapshot.splitViewHeight, accuracy: 1.5, file: file, line: line)
        XCTAssertEqual(snapshot.detailHeight, snapshot.splitViewHeight, accuracy: 1.5, file: file, line: line)
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

    private func firstTextScrollView(in view: NSView?) -> NSScrollView? {
        guard let view else { return nil }
        if let scrollView = view as? NSScrollView,
           scrollView.documentView is NSTextView {
            return scrollView
        }

        for subview in view.subviews {
            if let match = firstTextScrollView(in: subview) {
                return match
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
