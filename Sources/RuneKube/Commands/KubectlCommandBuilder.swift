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
            "get", "namespaces",
            "-o", "custom-columns=NAME:.metadata.name",
            "--no-headers"
        ]
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
            "-o", "custom-columns=NAME:.metadata.name,STATUS:.status.phase",
            "--no-headers"
        ]
    }

    public func podListAllNamespacesArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "pods",
            "-A",
            "-o", "custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,STATUS:.status.phase",
            "--no-headers"
        ]
    }

    public func deploymentListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "deployments",
            "-n", namespace,
            "-o", "json"
        ]
    }

    public func deploymentListAllNamespacesArguments(context: String) -> [String] {
        [
            "--context", context,
            "get", "deployments",
            "-A",
            "-o", "json"
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
            "-o", "custom-columns=NAME:.metadata.name",
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
        var args: [String] = [
            "--context", context,
            "logs",
            podName,
            "-n", namespace,
            "--timestamps"
        ]

        if let container {
            args += ["-c", container]
        }

        if filter.usesSinceTime {
            args += ["--since-time", filter.kubectlArgument]
        } else {
            args += ["--since", filter.kubectlArgument]
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
}
