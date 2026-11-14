import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    func genericResourceList(
        _ resources: [ClusterResourceSummary],
        selection: ClusterResourceSummary?,
        action: @escaping (ClusterResourceSummary?) -> Void
    ) -> some View {
        resourceListGate(
            kindTitle: viewModel.state.selectedWorkloadKind.title,
            visibleCount: resources.count
        ) {
            AppKitGenericResourceListView(
                kind: viewModel.state.selectedWorkloadKind,
                resources: resources,
                selectedResourceID: selection?.id,
                selectionRevision: viewModel.state.resourceSelectionRevision(
                    for: .resource(viewModel.state.selectedWorkloadKind)
                ),
                selectionRevisionAfterSelect: {
                    viewModel.state.resourceSelectionRevision(
                        for: .resource(viewModel.state.selectedWorkloadKind)
                    )
                },
                selectedResourceIDs: viewModel.state.selectedGenericResourceIDs,
                sortColumn: viewModel.genericResourceSortColumn,
                sortAscending: viewModel.genericResourceSortAscending,
                canApplyClusterMutations: viewModel.canApplyClusterMutations,
                isFavorite: { resource in
                    viewModel.isFavoriteResource(kind: resource.kind, namespace: resource.namespace, name: resource.name)
                },
                onSelectResource: { resource in
                    action(resource)
                },
                onToggleBulkSelection: viewModel.toggleGenericResourceBulkSelection,
                onToggleSort: viewModel.toggleGenericResourceSort,
                onToggleFavorite: { resource in
                    viewModel.toggleFavoriteResource(kind: resource.kind, namespace: resource.namespace, name: resource.name)
                },
                onOpenDescribe: { resource in
                    action(resource)
                    genericResourceManifestTab = .describe
                },
                onOpenYAML: { resource in
                    action(resource)
                    genericResourceManifestTab = .yaml
                },
                onDelete: { resource in
                    viewModel.requestDeleteResource(kind: resource.kind, name: resource.name)
                }
            )
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
        .id("\(viewModel.state.selectedSection.rawValue):\(viewModel.state.selectedWorkloadKind.kubernetesResourceName)")
        .transaction { transaction in
            transaction.animation = nil
        }
    }

    @ViewBuilder
    func resourceListGate<Content: View>(
        kindTitle: String,
        visibleCount: Int,
        scopeDescription: String? = nil,
        @ViewBuilder content: () -> Content
    ) -> some View {
        let presentation = ResourceListPresentation.project(
            isLoading: viewModel.state.isLoading,
            visibleCount: visibleCount,
            kindTitle: kindTitle,
            filterQuery: viewModel.state.resourceSearchQuery,
            scopeDescription: scopeDescription ?? resourceListScopeDescription,
            freshness: currentResourceListFreshness
        )

        switch presentation {
        case .content:
            content()
        case let .state(state, action):
            RunePaneContentStateView(
                state,
                style: .card,
                action: action.map { action in
                    switch action {
                    case .clearFilter:
                        return RuneContentStateAction("Clear Filter", systemImage: "xmark.circle") {
                            viewModel.setResourceSearchQuery("")
                        }
                    case .retry:
                        return RuneContentStateAction("Retry", systemImage: "arrow.clockwise") {
                            viewModel.refreshCurrentView(debounced: false)
                        }
                    }
                }
            )
        }
    }

    var resourceListScopeDescription: String {
        let namespace = viewModel.state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        return namespace.isEmpty ? "the current namespace scope" : "namespace \(namespace)"
    }

    var operatorResourceScopeDescription: String {
        let focusDescription = viewModel.operatorResourceFocus == .all
            ? "operator resources"
            : "the \(viewModel.operatorResourceFocus.title) focus"
        let namespaceDescription = viewModel.state.isHelmAllNamespaces
            ? "all namespaces"
            : resourceListScopeDescription
        return "\(focusDescription) in \(namespaceDescription)"
    }

    var currentResourceListCounts: (visible: Int, total: Int)? {
        guard showsResourceFilterControls else { return nil }
        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod: return (viewModel.visiblePods.count, viewModel.state.pods.count)
            case .deployment: return (viewModel.visibleDeployments.count, viewModel.state.deployments.count)
            default:
                return (viewModel.visibleGenericResourcesForBulkActions.count, currentGenericResourceTotalCount)
            }
        case .networking:
            if viewModel.state.selectedWorkloadKind == .service {
                return (viewModel.visibleServices.count, viewModel.state.services.count)
            }
            return (viewModel.visibleGenericResourcesForBulkActions.count, currentGenericResourceTotalCount)
        case .config, .storage, .rbac:
            return (viewModel.visibleGenericResourcesForBulkActions.count, currentGenericResourceTotalCount)
        case .events:
            return (viewModel.visibleEvents.count, viewModel.state.events.count)
        case .helm:
            switch effectiveHelmBrowserTab {
            case .releases:
                return (viewModel.visibleHelmReleases.count, viewModel.state.helmReleases.count)
            case .operatorResources:
                return (viewModel.visibleOperatorResources.count, viewModel.focusedOperatorResources.count)
            }
        case .overview, .terminal:
            return nil
        }
    }

    var currentGenericResourceTotalCount: Int {
        switch viewModel.state.selectedWorkloadKind {
        case .statefulSet: return viewModel.state.statefulSets.count
        case .daemonSet: return viewModel.state.daemonSets.count
        case .job: return viewModel.state.jobs.count
        case .cronJob: return viewModel.state.cronJobs.count
        case .replicaSet: return viewModel.state.replicaSets.count
        case .horizontalPodAutoscaler: return viewModel.state.horizontalPodAutoscalers.count
        case .endpoint: return viewModel.state.endpoints.count
        case .ingress: return viewModel.state.ingresses.count
        case .networkPolicy: return viewModel.state.networkPolicies.count
        case .configMap: return viewModel.state.configMaps.count
        case .secret: return viewModel.state.secrets.count
        case .persistentVolumeClaim: return viewModel.state.persistentVolumeClaims.count
        case .persistentVolume: return viewModel.state.persistentVolumes.count
        case .storageClass: return viewModel.state.storageClasses.count
        case .node: return viewModel.state.nodes.count
        case .serviceAccount: return viewModel.state.serviceAccounts.count
        case .role: return viewModel.state.rbacRoles.count
        case .roleBinding: return viewModel.state.rbacRoleBindings.count
        case .clusterRole: return viewModel.state.rbacClusterRoles.count
        case .clusterRoleBinding: return viewModel.state.rbacClusterRoleBindings.count
        case .pod, .deployment, .service, .event: return 0
        }
    }

    func resourceListCountText(visible: Int, total: Int) -> String {
        if visible != total {
            return "\(visible) of \(total)"
        }
        return "\(total) \(total == 1 ? "resource" : "resources")"
    }

    var currentResourceListScopeLabel: String {
        switch viewModel.state.selectedSection {
        case .events:
            return currentNamespaceScopeLabel
        case .helm:
            return viewModel.state.isHelmAllNamespaces ? "All namespaces" : currentNamespaceScopeLabel
        case .overview, .terminal:
            return "Cluster scope"
        default:
            if !viewModel.state.selectedWorkloadKind.isNamespaced {
                return "Cluster scope"
            }
            return currentNamespaceScopeLabel
        }
    }

    /// "Cluster scope" for lists the toolbar's namespace menu does not
    /// filter; nil when the namespace menu (or Helm's scope menu) already
    /// shows the scope.
    var currentResourceListScopeNote: String? {
        switch viewModel.state.selectedSection {
        case .events, .helm, .overview, .terminal:
            return nil
        default:
            return viewModel.state.selectedWorkloadKind.isNamespaced ? nil : "Cluster scope"
        }
    }

    var currentNamespaceScopeLabel: String {
        let namespace = viewModel.state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        return namespace.isEmpty ? "Namespace scope" : namespace
    }
}
