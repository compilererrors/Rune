import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    var overviewCardModules: [OverviewModule] {
        var modules: [OverviewModule] = [.pods, .deployments, .services, .ingresses, .configMaps, .cronJobs, .nodes]
        if !simpleMode {
            modules.append(.events)
        }
        return modules
    }

    struct OverviewCardPresentation {
        let title: String
        let count: Int
        let symbol: String
        let tint: Color
        let help: String?
    }

    func overviewCardPresentation(for module: OverviewModule) -> OverviewCardPresentation {
        switch module {
        case .pods:
            return OverviewCardPresentation(
                title: "Pods",
                count: viewModel.state.overviewPods.count,
                symbol: "cube.box.fill",
                tint: RuneCategoryColor.pods,
                help: nil
            )
        case .deployments:
            return OverviewCardPresentation(
                title: "Deployments",
                count: viewModel.state.overviewDeploymentsCount,
                symbol: "shippingbox.fill",
                tint: RuneCategoryColor.deployments,
                help: nil
            )
        case .services:
            return OverviewCardPresentation(
                title: "Services",
                count: viewModel.state.overviewServicesCount,
                symbol: "point.3.connected.trianglepath.dotted",
                tint: RuneCategoryColor.services,
                help: nil
            )
        case .ingresses:
            return OverviewCardPresentation(
                title: "Ingresses",
                count: viewModel.state.overviewIngressesCount,
                symbol: "network",
                tint: RuneCategoryColor.ingresses,
                help: nil
            )
        case .configMaps:
            return OverviewCardPresentation(
                title: "ConfigMaps",
                count: viewModel.state.overviewConfigMapsCount,
                symbol: "doc.text.fill",
                tint: RuneCategoryColor.configMaps,
                help: nil
            )
        case .cronJobs:
            return OverviewCardPresentation(
                title: "CronJobs",
                count: viewModel.state.overviewCronJobsCount,
                symbol: "calendar.badge.clock",
                tint: RuneCategoryColor.cronJobs,
                help: nil
            )
        case .nodes:
            return OverviewCardPresentation(
                title: "Nodes",
                count: viewModel.state.overviewNodesCount,
                symbol: "server.rack",
                tint: RuneCategoryColor.nodes,
                help: nil
            )
        case .events:
            return OverviewCardPresentation(
                title: "Events",
                count: viewModel.state.overviewEvents.count,
                symbol: "bolt.badge.clock.fill",
                tint: RuneCategoryColor.events,
                help: overviewEventsCardHelp
            )
        }
    }

    func overviewStatCard(module: OverviewModule, index: Int) -> some View {
        let presentation = overviewCardPresentation(for: module)
        return OverviewStatCard(
            title: presentation.title,
            count: presentation.count,
            symbol: presentation.symbol,
            tint: presentation.tint,
            isLoading: viewModel.state.isLoading,
            isKeyboardFocused: isOverviewCardKeyboardFocused(index),
            help: presentation.help,
            showsHoverHelp: showHoverTooltips
        ) {
            overviewCardSelectionIndex = index
            viewModel.openOverviewModule(module)
        }
    }

    func isOverviewCardKeyboardFocused(_ index: Int) -> Bool {
        keyboardPaneFocus == .content
            && viewModel.state.selectedSection == .overview
            && overviewCardSelectionIndex == index
    }

    var overviewPane: some View {
        // Another cluster's overview starts at the top, so its first panels are in view.
        ScrollViewReader { proxy in
        ScrollView {
            VStack(alignment: .leading, spacing: RuneUILayoutMetrics.contentSectionSpacing) {
                if !hasAvailableKubernetesContexts {
                    KubernetesConnectionOnboardingView(
                        favoriteImportedContexts: $viewModel.favoriteImportedKubeConfigContexts,
                        onImportFile: viewModel.importKubeConfig,
                        onPaste: viewModel.importKubeConfigFromPasteboard,
                        onImportFolder: viewModel.importKubeConfigFolder,
                        onUseDefault: viewModel.addDefaultKubeConfig,
                        onShowMoreOptions: {
                            addClusterPopoverPresented = true
                        }
                    )
                } else {
                    overviewStatusBanner
                    manualNamespaceBanner
                    if !simpleMode {
                        authDoctorPanel
                    }

                    RuneBalancedOverviewGrid(
                        minimumItemWidth: 160,
                        spacing: RuneUILayoutMetrics.contentControlSpacing
                    ) {
                        ForEach(Array(overviewCardModules.enumerated()), id: \.offset) { index, module in
                            overviewStatCard(module: module, index: index)
                        }
                    }

                    if !simpleMode {
                        OverviewClusterSignalsPanelView(
                            unhealthy: viewModel.overviewUnhealthyItems,
                            gitOpsRollups: viewModel.overviewGitOpsRollupItems,
                            incidents: viewModel.overviewIncidentTimelineItems,
                            dependencies: viewModel.overviewDependencyItems,
                            expandedPanels: $expandedOverviewInsightPanels,
                            onOpenSignal: viewModel.openOverviewSignal,
                            onOpenGitOpsRollup: viewModel.openOverviewGitOpsRollup,
                            onOpenDependency: viewModel.openOverviewDependency
                        )
                    }

                    VStack(alignment: .leading, spacing: RuneUILayoutMetrics.contentControlSpacing) {
                        RuneSectionHeader(
                            "Pod Health",
                            systemImage: "heart.text.square",
                            tint: RuneSemanticColorRole.success.color(in: activeAppearanceTheme.palette)
                        ) {
                            RuneHeaderCapsule(
                                "\(viewModel.state.overviewPods.count) total",
                                role: .value
                            )
                        }

                        RuneBadgeFlowLayout(
                            horizontalSpacing: RuneUILayoutMetrics.contentControlSpacing,
                            verticalSpacing: RuneUILayoutMetrics.contentControlSpacing
                        ) {
                            healthBadge(label: "Running", value: podStatusCount("running"), tone: .success)
                            healthBadge(label: "Pending", value: podStatusCount("pending"), tone: .warning)
                            healthBadge(label: "Failed", value: podStatusCount("failed"), tone: .danger)
                            healthBadge(label: "Other", value: max(0, viewModel.state.overviewPods.count - podStatusCount("running") - podStatusCount("pending") - podStatusCount("failed")), tone: .neutral)
                        }
                    }
                    .runePanelCard()

                    if !simpleMode {
                        OverviewRecentEventsPanelView(
                            events: viewModel.state.overviewEvents,
                            onOpenEventSource: viewModel.openEventSource
                        )
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .topLeading)
            .id(Self.overviewTopAnchor)
        }
        .id("overview")
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .onChange(of: viewModel.state.selectedContext?.name) { _, _ in
            proxy.scrollTo(Self.overviewTopAnchor, anchor: .top)
        }
        }
    }

    static let overviewTopAnchor = "overview-top"

    var workloadsPane: some View {
        workloadsContent
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    @ViewBuilder
    var workloadsContent: some View {
        switch viewModel.state.selectedWorkloadKind {
        case .pod:
            let pods = viewModel.visiblePods
            resourceListGate(kindTitle: "Pods", visibleCount: pods.count) {
                AppKitPodTableView(
                    pods: pods,
                    selectedPodID: viewModel.state.selectedPod?.id,
                    selectionRevision: viewModel.state.resourceSelectionRevision(for: .resource(.pod)),
                    selectionRevisionAfterSelect: {
                        viewModel.state.resourceSelectionRevision(for: .resource(.pod))
                    },
                    selectedPodIDs: viewModel.state.selectedPodIDs,
                    sortColumn: viewModel.podSortColumn,
                    sortAscending: viewModel.podSortAscending,
                    nameColumnWidth: podNameColumnWidth,
                    canApplyClusterMutations: viewModel.canApplyClusterMutations,
                    isFavorite: viewModel.podFavoriteMatcher(),
                    onSelectPod: viewModel.selectPod,
                    onToggleBulkSelection: viewModel.togglePodBulkSelection,
                    onToggleSort: viewModel.togglePodSort,
                    onNameColumnWidthChanged: commitPodNameColumnWidth,
                    onToggleFavorite: { pod in
                        viewModel.toggleFavoriteResource(kind: .pod, namespace: pod.namespace, name: pod.name)
                    },
                    onOpenLogs: { pod in
                        viewModel.selectPod(pod)
                        podInspectorTab = .logs
                        viewModel.reloadLogsForSelection()
                    },
                    onOpenExec: { pod in
                        viewModel.selectPod(pod)
                        podInspectorTab = .exec
                    },
                    onOpenDescribe: { pod in
                        viewModel.selectPod(pod)
                        podInspectorTab = .describe
                    },
                    onOpenYAML: { pod in
                        viewModel.selectPod(pod)
                        podInspectorTab = .yaml
                    },
                    onDelete: { pod in
                        viewModel.requestDeleteResource(kind: .pod, name: pod.name)
                    }
                )
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                .transaction { transaction in
                    transaction.animation = nil
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .id("workloads:pods")

        case .deployment:
            let deployments = viewModel.visibleDeployments
            resourceListGate(kindTitle: "Deployments", visibleCount: deployments.count) {
                AppKitDeploymentListView(
                    deployments: deployments,
                    selectedDeploymentID: viewModel.state.selectedDeployment?.id,
                    selectionRevision: viewModel.state.resourceSelectionRevision(for: .resource(.deployment)),
                    selectionRevisionAfterSelect: {
                        viewModel.state.resourceSelectionRevision(for: .resource(.deployment))
                    },
                    sortColumn: viewModel.deploymentSortColumn,
                    sortAscending: viewModel.deploymentSortAscending,
                    canApplyClusterMutations: viewModel.canApplyClusterMutations,
                    isFavorite: { deployment in
                        viewModel.isFavoriteResource(kind: .deployment, namespace: deployment.namespace, name: deployment.name)
                    },
                    onSelectDeployment: viewModel.selectDeployment,
                    onToggleSort: viewModel.toggleDeploymentSort,
                    onToggleFavorite: { deployment in
                        viewModel.toggleFavoriteResource(kind: .deployment, namespace: deployment.namespace, name: deployment.name)
                    },
                    onOpenUnifiedLogs: { deployment in
                        viewModel.selectDeployment(deployment)
                        deploymentInspectorTab = .unifiedLogs
                        viewModel.reloadLogsForSelection()
                    },
                    onOpenRollout: { deployment in
                        viewModel.selectDeployment(deployment)
                        deploymentInspectorTab = .rollout
                    },
                    onOpenDescribe: { deployment in
                        viewModel.selectDeployment(deployment)
                        deploymentInspectorTab = .describe
                    },
                    onOpenYAML: { deployment in
                        viewModel.selectDeployment(deployment)
                        deploymentInspectorTab = .yaml
                    },
                    onDelete: { deployment in
                        viewModel.requestDeleteResource(kind: .deployment, name: deployment.name)
                    }
                )
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            }

        case .statefulSet:
            genericResourceList(viewModel.visibleStatefulSets, selection: viewModel.state.selectedStatefulSet, action: viewModel.selectStatefulSet)

        case .daemonSet:
            genericResourceList(viewModel.visibleDaemonSets, selection: viewModel.state.selectedDaemonSet, action: viewModel.selectDaemonSet)

        case .job:
            genericResourceList(viewModel.visibleJobs, selection: viewModel.state.selectedJob, action: viewModel.selectJob)

        case .cronJob:
            genericResourceList(viewModel.visibleCronJobs, selection: viewModel.state.selectedCronJob, action: viewModel.selectCronJob)

        case .replicaSet:
            genericResourceList(viewModel.visibleReplicaSets, selection: viewModel.state.selectedReplicaSet, action: viewModel.selectReplicaSet)

        case .horizontalPodAutoscaler:
            genericResourceList(
                viewModel.visibleHorizontalPodAutoscalers,
                selection: viewModel.state.selectedHorizontalPodAutoscaler,
                action: viewModel.selectHorizontalPodAutoscaler
            )

        case .service, .endpoint, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            EmptyView()

        case .event:
            EmptyView()
        }
    }

    var networkingPane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                let services = viewModel.visibleServices
                resourceListGate(kindTitle: "Services", visibleCount: services.count) {
                    AppKitServiceListView(
                        services: services,
                        selectedServiceID: viewModel.state.selectedService?.id,
                        selectionRevision: viewModel.state.resourceSelectionRevision(for: .resource(.service)),
                        selectionRevisionAfterSelect: {
                            viewModel.state.resourceSelectionRevision(for: .resource(.service))
                        },
                        sortColumn: viewModel.serviceSortColumn,
                        sortAscending: viewModel.serviceSortAscending,
                        canApplyClusterMutations: viewModel.canApplyClusterMutations,
                        isFavorite: { service in
                            viewModel.isFavoriteResource(kind: .service, namespace: service.namespace, name: service.name)
                        },
                        onSelectService: viewModel.selectService,
                        onToggleSort: viewModel.toggleServiceSort,
                        onToggleFavorite: { service in
                            viewModel.toggleFavoriteResource(kind: .service, namespace: service.namespace, name: service.name)
                        },
                        onOpenUnifiedLogs: { service in
                            viewModel.selectService(service)
                            serviceInspectorTab = .unifiedLogs
                            viewModel.reloadLogsForSelection()
                        },
                        onOpenPortForward: { service in
                            viewModel.selectService(service)
                            serviceInspectorTab = .portForward
                        },
                        onOpenDescribe: { service in
                            viewModel.selectService(service)
                            serviceInspectorTab = .describe
                        },
                        onOpenYAML: { service in
                            viewModel.selectService(service)
                            serviceInspectorTab = .yaml
                        },
                        onDelete: { service in
                            viewModel.requestDeleteResource(kind: .service, name: service.name)
                        }
                    )
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                    .id("networking:service")
                }

            case .ingress:
                genericResourceList(viewModel.visibleIngresses, selection: viewModel.state.selectedIngress, action: viewModel.selectIngress)

            case .endpoint:
                genericResourceList(viewModel.visibleEndpoints, selection: viewModel.state.selectedEndpoint, action: viewModel.selectEndpoint)

            case .networkPolicy:
                genericResourceList(
                    viewModel.visibleNetworkPolicies,
                    selection: viewModel.state.selectedNetworkPolicy,
                    action: viewModel.selectNetworkPolicy
                )

            default:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    var configPane: some View {
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
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    var storagePane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .persistentVolumeClaim:
                genericResourceList(
                    viewModel.visiblePersistentVolumeClaims,
                    selection: viewModel.state.selectedPersistentVolumeClaim,
                    action: viewModel.selectPersistentVolumeClaim
                )

            case .persistentVolume:
                genericResourceList(
                    viewModel.visiblePersistentVolumes,
                    selection: viewModel.state.selectedPersistentVolume,
                    action: viewModel.selectPersistentVolume
                )

            case .storageClass:
                genericResourceList(
                    viewModel.visibleStorageClasses,
                    selection: viewModel.state.selectedStorageClass,
                    action: viewModel.selectStorageClass
                )

            case .node:
                genericResourceList(viewModel.visibleNodes, selection: viewModel.state.selectedNode, action: viewModel.selectNode)

            default:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    var rbacPane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                genericResourceList(
                    viewModel.visibleRBACResources,
                    selection: viewModel.state.selectedRBACResource,
                    action: viewModel.selectRBACResource
                )
            default:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    var helmPane: some View {
        Group {
            switch effectiveHelmBrowserTab {
            case .releases:
                helmReleaseBrowser
            case .operatorResources:
                operatorResourceViews
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    var helmReleaseBrowser: some View {
        let releases = viewModel.visibleHelmReleases
        return resourceListGate(
            kindTitle: "Helm releases",
            visibleCount: releases.count,
            scopeDescription: viewModel.state.isHelmAllNamespaces ? "all namespaces" : resourceListScopeDescription
        ) {
            AppKitHelmReleaseListView(
                releases: releases,
                selectedReleaseID: viewModel.state.selectedHelmRelease?.id,
                selectionRevision: viewModel.state.resourceSelectionRevision(for: .helmRelease),
                selectionRevisionAfterSelect: {
                    viewModel.state.resourceSelectionRevision(for: .helmRelease)
                },
                sortColumn: viewModel.helmReleaseSortColumn,
                sortAscending: viewModel.helmReleaseSortAscending,
                onSelectRelease: { release in
                    helmBrowserTab = .releases
                    viewModel.selectHelmRelease(release)
                },
                onToggleSort: viewModel.toggleHelmReleaseSort
            )
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    var operatorResourceViews: some View {
        let resources = viewModel.visibleOperatorResources
        return resourceListGate(
            kindTitle: viewModel.operatorResourceFocus == .all
                ? "Operator resources"
                : "\(viewModel.operatorResourceFocus.title) operator resources",
            visibleCount: resources.count,
            scopeDescription: operatorResourceScopeDescription
        ) {
            AppKitOperatorResourceListView(
                resources: viewModel.pagedOperatorResources,
                selectedResourceID: viewModel.state.selectedOperatorResource?.id,
                selectionRevision: viewModel.state.resourceSelectionRevision(for: .operatorResource),
                selectionRevisionAfterSelect: {
                    viewModel.state.resourceSelectionRevision(for: .operatorResource)
                },
                sortColumn: viewModel.operatorResourceSortColumn,
                sortAscending: viewModel.operatorResourceSortAscending,
                showsPrinterColumns: viewModel.showsOperatorPrinterColumnsForCurrentFamily,
                isFavorite: viewModel.isFavoriteOperatorResource,
                onSelectResource: { resource in
                    helmBrowserTab = .operatorResources
                    viewModel.selectOperatorResource(resource)
                    genericResourceManifestTab = .overview
                    yamlManifestIsEditing = false
                },
                onToggleSort: viewModel.toggleOperatorResourceSort,
                onToggleFavorite: viewModel.toggleFavoriteOperatorResource,
                onOpenDescribe: { resource in
                    helmBrowserTab = .operatorResources
                    viewModel.selectOperatorResource(resource)
                    genericResourceManifestTab = .describe
                    yamlManifestIsEditing = false
                },
                onOpenYAML: { resource in
                    helmBrowserTab = .operatorResources
                    viewModel.selectOperatorResource(resource)
                    genericResourceManifestTab = .yaml
                    yamlManifestIsEditing = false
                }
            )
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    var eventsPane: some View {
        let events = viewModel.visibleEvents
        return resourceListGate(kindTitle: "Events", visibleCount: events.count) {
            AppKitEventListView(
                events: events,
                selectedEventID: viewModel.state.selectedEvent?.id,
                selectionRevision: viewModel.state.resourceSelectionRevision(for: .resource(.event)),
                selectionRevisionAfterSelect: {
                    viewModel.state.resourceSelectionRevision(for: .resource(.event))
                },
                sortColumn: viewModel.eventSortColumn,
                sortAscending: viewModel.eventSortAscending,
                onSelectEvent: viewModel.selectEvent,
                onToggleSort: viewModel.toggleEventSort
            )
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .runeHelp(viewModel.state.selectedEvent.map(eventHint(for:)) ?? "", enabled: showHoverTooltips)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }
}
