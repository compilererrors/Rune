import Foundation
import RuneCore

@MainActor
public final class ResourceStore {
    private var podsByContextAndNamespace: [String: [PodSummary]] = [:]

    public init() {}

    public func cachePods(_ pods: [PodSummary], context: KubeContext, namespace: String) {
        podsByContextAndNamespace[key(context: context, namespace: namespace)] = pods
    }

    public func pods(context: KubeContext, namespace: String) -> [PodSummary] {
        podsByContextAndNamespace[key(context: context, namespace: namespace)] ?? []
    }

    private func key(context: KubeContext, namespace: String) -> String {
        "\(context.name)::\(namespace)"
    }
}
