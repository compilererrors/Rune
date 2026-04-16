import Foundation

public struct HelmCommandBuilder {
    public init() {}

    public func listArguments(context: String, namespace: String?, allNamespaces: Bool) -> [String] {
        var args = baseArguments(context: context)
        args += ["list", "-o", "json"]

        if allNamespaces {
            args.append("--all-namespaces")
        } else if let namespace, !namespace.isEmpty {
            args += ["--namespace", namespace]
        }

        return args
    }

    public func valuesArguments(context: String, namespace: String, releaseName: String) -> [String] {
        baseArguments(context: context) + ["get", "values", releaseName, "--all", "--namespace", namespace]
    }

    public func manifestArguments(context: String, namespace: String, releaseName: String) -> [String] {
        baseArguments(context: context) + ["get", "manifest", releaseName, "--namespace", namespace]
    }

    public func historyArguments(context: String, namespace: String, releaseName: String) -> [String] {
        baseArguments(context: context) + ["history", releaseName, "--namespace", namespace, "-o", "json"]
    }

    public func rollbackArguments(context: String, namespace: String, releaseName: String, revision: Int) -> [String] {
        baseArguments(context: context) + ["rollback", releaseName, String(revision), "--namespace", namespace, "--wait"]
    }

    private func baseArguments(context: String) -> [String] {
        ["--kube-context", context]
    }
}
