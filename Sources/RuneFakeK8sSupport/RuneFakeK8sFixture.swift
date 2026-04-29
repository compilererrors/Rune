import Foundation

public struct RuneFakeK8sFixture: Sendable {
    public let contexts: [RuneFakeK8sCluster]

    public init(contexts: [RuneFakeK8sCluster] = RuneFakeK8sFixture.defaultContexts) {
        self.contexts = contexts
    }

    public static let defaultContextName = "fake-orbit-mesh"

    public static let defaultContexts: [RuneFakeK8sCluster] = [
        RuneFakeK8sCluster(
            contextName: "fake-orbit-mesh",
            defaultNamespace: "alpha-zone",
            namespaces: [
                RuneFakeK8sNamespace(
                    name: "alpha-zone",
                    pods: [
                        RuneFakeK8sPod(
                            name: "orbit-lens-6f58d7d89b-hx9q2",
                            deploymentName: "orbit-lens",
                            phase: "Running",
                            restarts: 1,
                            cpu: "42m",
                            memory: "96Mi",
                            podIP: "10.42.0.10",
                            nodeName: "orbit-node-a",
                            labels: ["app": "orbit-lens", "tier": "alpha-zone"],
                            containers: ["lens"]
                        ),
                        RuneFakeK8sPod(
                            name: "ember-gate-75c9f746b8-kq2wm",
                            deploymentName: "ember-gate",
                            phase: "Running",
                            restarts: 0,
                            cpu: "31m",
                            memory: "88Mi",
                            podIP: "10.42.0.11",
                            nodeName: "orbit-node-b",
                            labels: ["app": "ember-gate", "tier": "alpha-zone"],
                            containers: ["gate", "metrics"]
                        )
                    ],
                    deployments: [
                        RuneFakeK8sDeployment(name: "ember-gate", readyReplicas: 1, replicas: 2, selector: ["app": "ember-gate"]),
                        RuneFakeK8sDeployment(name: "orbit-lens", readyReplicas: 2, replicas: 2, selector: ["app": "orbit-lens"])
                    ],
                    services: [
                        RuneFakeK8sService(name: "ember-gate", selector: ["app": "ember-gate"], clusterIP: "10.96.0.21"),
                        RuneFakeK8sService(name: "orbit-lens", selector: ["app": "orbit-lens"], clusterIP: "10.96.0.20")
                    ]
                ),
                RuneFakeK8sNamespace(
                    name: "bravo-zone",
                    pods: [
                        RuneFakeK8sPod(
                            name: "bravo-spoke-59fd6dfb4b-s9n2p",
                            deploymentName: "bravo-spoke",
                            phase: "Pending",
                            restarts: 0,
                            cpu: "0m",
                            memory: "0Mi",
                            podIP: nil,
                            nodeName: "orbit-node-c",
                            labels: ["app": "bravo-spoke", "tier": "bravo-zone"],
                            containers: ["spoke"]
                        )
                    ],
                    deployments: [
                        RuneFakeK8sDeployment(name: "bravo-spoke", readyReplicas: 0, replicas: 1, selector: ["app": "bravo-spoke"])
                    ],
                    services: [
                        RuneFakeK8sService(name: "bravo-spoke", selector: ["app": "bravo-spoke"], clusterIP: "10.96.1.20")
                    ]
                )
            ],
            nodes: [
                RuneFakeK8sNode(name: "orbit-node-a", internalIP: "10.10.0.11", cpu: "410m", memory: "1820Mi"),
                RuneFakeK8sNode(name: "orbit-node-b", internalIP: "10.10.0.12", cpu: "360m", memory: "1710Mi"),
                RuneFakeK8sNode(name: "orbit-node-c", internalIP: "10.10.0.13", cpu: "390m", memory: "1640Mi")
            ]
        )
    ]

    public func cluster(named contextName: String?) -> RuneFakeK8sCluster? {
        let name = contextName?.isEmpty == false ? contextName! : Self.defaultContextName
        return contexts.first { $0.contextName == name } ?? contexts.first
    }
}

public struct RuneFakeK8sCluster: Sendable {
    public let contextName: String
    public let defaultNamespace: String
    public let namespaces: [RuneFakeK8sNamespace]
    public let nodes: [RuneFakeK8sNode]
}

public struct RuneFakeK8sNamespace: Sendable {
    public let name: String
    public let pods: [RuneFakeK8sPod]
    public let deployments: [RuneFakeK8sDeployment]
    public let services: [RuneFakeK8sService]
}

public struct RuneFakeK8sPod: Sendable {
    public let name: String
    public let deploymentName: String
    public let phase: String
    public let restarts: Int
    public let cpu: String
    public let memory: String
    public let podIP: String?
    public let nodeName: String
    public let labels: [String: String]
    public let containers: [String]
}

public struct RuneFakeK8sDeployment: Sendable {
    public let name: String
    public let readyReplicas: Int
    public let replicas: Int
    public let selector: [String: String]
}

public struct RuneFakeK8sService: Sendable {
    public let name: String
    public let selector: [String: String]
    public let clusterIP: String
}

public struct RuneFakeK8sNode: Sendable {
    public let name: String
    public let internalIP: String
    public let cpu: String
    public let memory: String
}
