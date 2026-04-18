import Foundation
import RuneCore

public struct KubectlCommandBuilder {
    public init() {}

    public func contextListArguments() -> [String] {
        ["config", "get-contexts", "-o", "name"]
    }

    public func namespaceListArguments(context: String) -> [String] {
        [
            "--context", context,
            "--request-timeout=90s",
            "get", "namespaces",
            "-o", "custom-columns=NAME:.metadata.name",
            "--no-headers"
        ]
    }

    /// `kubectl get --raw` path for a **namespaced** collection List with `limit=1` so the payload stays tiny; total size comes from `metadata.remainingItemCount` (see `KubectlListJSON.collectionListTotal`).
    /// Paths are built with ``KubernetesRESTPath`` (same REST layout as client-go / Swiftkube-style clients).
    public func namespacedResourceListMetadataAPIPath(namespace: String, resource: String) -> String? {
        KubernetesRESTPath.namespacedCollectionMetadataProbe(namespace: namespace, resource: resource)
    }

    /// Cluster-scoped List (e.g. nodes) with `limit=1` for cheap total via `remainingItemCount`.
    public func clusterResourceListMetadataAPIPath(resource: String) -> String? {
        KubernetesRESTPath.clusterCollectionMetadataProbe(resource: resource)
    }

    public func rawGetArguments(context: String, apiPath: String) -> [String] {
        ["--context", context, "get", "--raw", apiPath]
    }

    /// Same as ``rawGetArguments(context:apiPath:)`` but typed; swap transport later without changing call sites.
    public func rawGetArguments(context: String, request: KubernetesRESTRequest) -> [String] {
        rawGetArguments(context: context, apiPath: request.apiPath)
    }

    public func contextNamespaceArguments(context: String) -> [String] {
        [
            "config", "view",
            "--minify",
            "--context", context,
            "-o", "jsonpath={..namespace}"
        ]
    }

    public func podListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "pods",
            "-n", namespace,
            "--chunk-size=200",
            "--request-timeout=20s",
            "-o", "json"
        ]
    }

    public func podStatusListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "pods",
            "-n", namespace,
            "--chunk-size=200",
            "--request-timeout=90s",
            "-o", "custom-columns=NAME:.metadata.name,STATUS:.status.phase",
            "--no-headers"
        ]
    }

    public func podListAllNamespacesArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "pods",
            "-A",
            "--chunk-size=200",
            "--request-timeout=20s",
            "-o", "json"
        ]
    }

    /// `kubectl top pods` — needs metrics-server; may fail harmlessly when unavailable.
    public func podTopArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "top", "pods",
            "-n", namespace,
            "--no-headers"
        ]
    }

    public func podTopAllNamespacesArguments(context: String) -> [String] {
        [
            "--context", context,
            "top", "pods",
            "-A",
            "--no-headers"
        ]
    }

    /// Text table fallback when JSON list fails (timeout/parse error). Includes restarts + creationTimestamp.
    public func podListTextArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "pods",
            "-n", namespace,
            "--chunk-size=200",
            "--request-timeout=20s",
            "-o", "custom-columns=NAME:.metadata.name,STATUS:.status.phase,RESTARTS:.status.containerStatuses[*].restartCount,CREATED:.metadata.creationTimestamp",
            "--no-headers"
        ]
    }

    public func podListAllNamespacesTextArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "pods",
            "-A",
            "--chunk-size=200",
            "--request-timeout=20s",
            "-o", "custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,STATUS:.status.phase,RESTARTS:.status.containerStatuses[*].restartCount,CREATED:.metadata.creationTimestamp",
            "--no-headers"
        ]
    }

    public func deploymentListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "deployments",
            "-n", namespace,
            "--chunk-size=200",
            "--request-timeout=20s",
            "-o", "json"
        ]
    }

    public func deploymentListTextArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "deployments",
            "-n", namespace,
            "--chunk-size=200",
            "--request-timeout=20s",
            "-o", "custom-columns=NAME:.metadata.name,READY:.status.readyReplicas,DESIRED:.spec.replicas",
            "--no-headers"
        ]
    }

    public func deploymentListAllNamespacesArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "deployments",
            "-A",
            "--chunk-size=200",
            "--request-timeout=20s",
            "-o", "json"
        ]
    }

    public func deploymentListAllNamespacesTextArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "deployments",
            "-A",
            "--chunk-size=200",
            "--request-timeout=20s",
            "-o", "custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,READY:.status.readyReplicas,DESIRED:.spec.replicas",
            "--no-headers"
        ]
    }

    public func statefulSetListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "statefulsets",
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func daemonSetListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "daemonsets",
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func jobListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "jobs",
            "-n", namespace,
            "--chunk-size=200",
            "--request-timeout=90s",
            "-o", "json"
        ]
    }

    /// Lightweight list for the workloads table (same layering idea as `podListTextArguments`).
    public func jobListTextArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "jobs",
            "-n", namespace,
            "--chunk-size=200",
            "--request-timeout=90s",
            "-o", "custom-columns=NAME:.metadata.name,SUCCEEDED:.status.succeeded,ACTIVE:.status.active,FAILED:.status.failed",
            "--no-headers"
        ]
    }

    public func cronJobListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "cronjobs",
            "-n", namespace,
            "--chunk-size=200",
            "--request-timeout=90s",
            "-o", "json"
        ]
    }

    /// Schedules can contain spaces; output is tab-separated between columns.
    public func cronJobListTextArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "cronjobs",
            "-n", namespace,
            "--chunk-size=200",
            "--request-timeout=90s",
            "-o", "custom-columns=NAME:.metadata.name,SCHEDULE:.spec.schedule,SUSPEND:.spec.suspend",
            "--no-headers"
        ]
    }

    public func replicaSetListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "replicasets",
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func persistentVolumeClaimListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "pvc",
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func persistentVolumeListArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "pv",
            "-o", "json"
        ]
    }

    public func storageClassListArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "storageclass",
            "-o", "json"
        ]
    }

    public func horizontalPodAutoscalerListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "hpa",
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func networkPolicyListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "networkpolicy",
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func patchCronJobSuspendArguments(context: String, namespace: String, name: String, suspend: Bool) -> [String] {
        let patch = #"{"spec":{"suspend":\#(suspend ? "true" : "false")}}"#
        return [
            "--context", context,
            "patch", "cronjob", name,
            "-n", namespace,
            "--type", "merge",
            "-p", patch
        ]
    }

    public func createJobFromCronJobArguments(context: String, namespace: String, cronJobName: String, jobName: String) -> [String] {
        [
            "--context", context,
            "create", "job", jobName,
            "-n", namespace,
            "--from=cronjob/\(cronJobName)"
        ]
    }

    public func serviceListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "services",
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func serviceListAllNamespacesArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "services",
            "-A",
            "-o", "json"
        ]
    }

    public func ingressListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "ingresses",
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func ingressListAllNamespacesArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "ingresses",
            "-A",
            "-o", "json"
        ]
    }

    public func configMapListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "configmaps",
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func configMapListAllNamespacesArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "configmaps",
            "-A",
            "-o", "json"
        ]
    }

    public func secretListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "secrets",
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func nodeListArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "nodes",
            "-o", "json"
        ]
    }

    public func eventListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "events",
            "-n", namespace,
            "--sort-by=.lastTimestamp",
            "-o", "json"
        ]
    }

    public func eventListAllNamespacesArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "events",
            "-A",
            "--sort-by=.lastTimestamp",
            "-o", "json"
        ]
    }

    public func namespacedResourceCountArguments(
        context: String,
        namespace: String,
        resource: String
    ) -> [String] {
        [
            "--context", context,
            "get", resource,
            "-n", namespace,
            "--chunk-size=200",
            "--request-timeout=15s",
            "-o", "custom-columns=NAME:.metadata.name",
            "--no-headers"
        ]
    }

    public func clusterResourceCountArguments(
        context: String,
        resource: String
    ) -> [String] {
        [
            "--context", context,
            "get", resource,
            "--chunk-size=200",
            "--request-timeout=15s",
            "-o", "custom-columns=NAME:.metadata.name",
            "--no-headers"
        ]
    }

    /// `kubectl top nodes` — cluster usage snapshot with built-in CPU% and MEM%.
    public func nodeTopArguments(context: String) -> [String] {
        [
            "--context", context,
            "top", "nodes",
            "--no-headers"
        ]
    }

    public func podLogsArguments(
        context: String,
        namespace: String,
        podName: String,
        container: String?,
        filter: LogTimeFilter,
        previous: Bool,
        follow: Bool
    ) -> [String] {
        // Client-side ceiling so the apiserver does not hang indefinitely (process runner also enforces a timeout).
        var args: [String] = [
            "--context", context,
            "--request-timeout", "55s",
            "logs",
            podName,
            "-n", namespace,
            "--timestamps"
        ]

        if let container {
            args += ["-c", container]
        }

        switch filter {
        case .all:
            // Plain `kubectl logs` feel: bounded tail (full stream is often huge / times out).
            args.append("--tail=200")
        case let .tailLines(lines):
            args.append("--tail=\(max(1, lines))")
        case .lastMinutes, .lastHours, .lastDays, .since:
            if let since = filter.kubectlSinceArgument {
                if filter.usesSinceTime {
                    args += ["--since-time", since]
                } else {
                    args += ["--since", since]
                }
            }
            // Cap bytes returned when using a time window (avoids multi-minute transfers).
            args.append("--tail=5000")
        }

        if previous {
            args.append("--previous")
        }

        if follow {
            args.append("-f")
        }

        return args
    }

    public func resourceYAMLArguments(context: String, namespace: String, kind: KubeResourceKind, name: String) -> [String] {
        var args = [
            "--context", context,
            "get", kind.kubectlName, name
        ]

        if kind.isNamespaced {
            args += ["-n", namespace]
        }

        args += ["-o", "yaml"]
        return args
    }

    /// Full object JSON for inspector/detail merges (small payload vs listing all resources).
    public func resourceJSONArguments(context: String, namespace: String, kind: KubeResourceKind, name: String) -> [String] {
        var args = [
            "--context", context,
            "get", kind.kubectlName, name
        ]

        if kind.isNamespaced {
            args += ["-n", namespace]
        }

        args += ["-o", "json"]
        return args
    }

    /// Human-readable `kubectl describe` output for the describe inspector.
    public func describeResourceArguments(context: String, namespace: String, kind: KubeResourceKind, name: String) -> [String] {
        var args = [
            "--context", context,
            "describe", kind.kubectlName, name
        ]

        if kind.isNamespaced {
            args += ["-n", namespace]
        }

        return args
    }

    public func podExecArguments(
        context: String,
        namespace: String,
        podName: String,
        container: String?,
        command: [String]
    ) -> [String] {
        var args: [String] = [
            "--context", context,
            "exec", podName,
            "-n", namespace
        ]

        if let container, !container.isEmpty {
            args += ["-c", container]
        }

        args.append("--")
        args += command
        return args
    }

    public func serviceJSONArguments(context: String, namespace: String, serviceName: String) -> [String] {
        [
            "--context", context,
            "get", "service", serviceName,
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func deploymentJSONArguments(context: String, namespace: String, deploymentName: String) -> [String] {
        [
            "--context", context,
            "get", "deployment", deploymentName,
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func podsByLabelSelectorArguments(context: String, namespace: String, selector: String) -> [String] {
        [
            "--context", context,
            "get", "pods",
            "-n", namespace,
            "-l", selector,
            "-o", "json"
        ]
    }

    public func podsByLabelSelectorTextArguments(context: String, namespace: String, selector: String) -> [String] {
        [
            "--context", context,
            "get", "pods",
            "-n", namespace,
            "-l", selector,
            "-o", "custom-columns=NAME:.metadata.name,STATUS:.status.phase",
            "--no-headers"
        ]
    }

    public func deleteResourceArguments(context: String, namespace: String, kind: KubeResourceKind, name: String) -> [String] {
        var args = [
            "--context", context,
            "delete", kind.kubectlName, name
        ]

        if kind.isNamespaced {
            args += ["-n", namespace]
        }

        return args
    }

    public func scaleDeploymentArguments(context: String, namespace: String, deploymentName: String, replicas: Int) -> [String] {
        [
            "--context", context,
            "scale", "deployment", deploymentName,
            "-n", namespace,
            "--replicas", String(replicas)
        ]
    }

    public func rolloutRestartArguments(context: String, namespace: String, deploymentName: String) -> [String] {
        [
            "--context", context,
            "rollout", "restart", "deployment", deploymentName,
            "-n", namespace
        ]
    }

    public func rolloutHistoryArguments(context: String, namespace: String, deploymentName: String) -> [String] {
        [
            "--context", context,
            "rollout", "history", "deployment", deploymentName,
            "-n", namespace
        ]
    }

    public func rolloutUndoArguments(context: String, namespace: String, deploymentName: String, revision: Int?) -> [String] {
        var args = [
            "--context", context,
            "rollout", "undo", "deployment", deploymentName,
            "-n", namespace
        ]

        if let revision {
            args += ["--to-revision", String(revision)]
        }

        return args
    }

    public func portForwardArguments(
        context: String,
        namespace: String,
        targetKind: PortForwardTargetKind,
        targetName: String,
        localPort: Int,
        remotePort: Int,
        address: String
    ) -> [String] {
        [
            "--context", context,
            "port-forward",
            "\(targetKind.kubectlResourceName)/\(targetName)",
            "\(localPort):\(remotePort)",
            "-n", namespace,
            "--address", address
        ]
    }

    public func applyFileArguments(context: String, namespace: String, filePath: String) -> [String] {
        [
            "--context", context,
            "apply",
            "-n", namespace,
            "-f", filePath
        ]
    }

    public func roleListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "roles",
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func roleBindingListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "rolebindings",
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func clusterRoleListArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "clusterroles",
            "--request-timeout=60s",
            "-o", "json"
        ]
    }

    public func clusterRoleBindingListArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "clusterrolebindings",
            "--request-timeout=60s",
            "-o", "json"
        ]
    }

    /// Fallback when JSON list fails (timeout, partial output).
    public func clusterRoleListTextArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "clusterroles",
            "--request-timeout=60s",
            "-o", "custom-columns=NAME:.metadata.name",
            "--no-headers"
        ]
    }

    public func clusterRoleBindingListTextArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "clusterrolebindings",
            "--request-timeout=60s",
            "-o", "custom-columns=NAME:.metadata.name",
            "--no-headers"
        ]
    }
}
