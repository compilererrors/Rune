import Foundation
import RuneCore

public struct KubectlCommandBuilder {
    public init() {}

    public func contextListArguments() -> [String] {
        ["config", "get-contexts", "-o", "name"]
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

    public func deploymentListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "deployments",
            "-n", namespace,
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

    public func ingressListArguments(context: String, namespace: String) -> [String] {
        [
            "--context", context,
            "get", "ingresses",
            "-n", namespace,
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
