import Foundation
import RuneFakeK8sSupport

struct CommandFailure: Error, CustomStringConvertible {
    let message: String

    var description: String { message }
}

struct NodeFixture {
    let name: String
    let hostIP: String
    let cpu: String
    let cpuPercent: String
    let memory: String
    let memoryPercent: String
}

struct NamespaceFixture {
    let name: String
    let deploymentNames: [String]
    let statefulSetNames: [String]
    let daemonSetNames: [String]
    let cronJobNames: [String]
    let ingressTargets: [String]
    let configMapNames: [String]
    let secretNames: [String]
    let networkPolicyNames: [String]
    let roleNames: [String]
    let roleBindingNames: [String]
}

struct ClusterFixture {
    let contextName: String
    let defaultNamespace: String
    let namespaces: [NamespaceFixture]
    let nodes: [NodeFixture]
}

struct PodFixture {
    let name: String
    let namespace: String
    let workloadName: String
    let ownerKind: String
    let phase: String
    let restarts: Int
    let createdAt: String
    let cpu: String
    let memory: String
    let nodeName: String
    let hostIP: String
    let podIP: String
    let labels: [String: String]
    let containers: [String]
}

private let now = Date(timeIntervalSince1970: 1_776_718_400) // 2026-04-21T00:00:00Z

private func timestamp(daysAgo: Int, hoursAgo: Int = 0, minutesAgo: Int = 0) -> String {
    let seconds = TimeInterval(((daysAgo * 24) + hoursAgo) * 3600 + minutesAgo * 60)
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime]
    formatter.timeZone = TimeZone(secondsFromGMT: 0)
    return formatter.string(from: now.addingTimeInterval(-seconds))
}

private func stableNumber(_ seed: String) -> UInt64 {
    var value: UInt64 = 0xcbf29ce484222325
    for byte in seed.utf8 {
        value ^= UInt64(byte)
        value &*= 0x100000001b3
    }
    return value
}

private func stableInt(_ seed: String, modulo: Int) -> Int {
    Int(stableNumber(seed) % UInt64(max(1, modulo)))
}

private func stableToken(_ seed: String, length: Int) -> String {
    let alphabet = Array("bcdfghjklmnpqrstvwxyz23456789")
    let start = stableInt(seed, modulo: alphabet.count)
    return String((0..<length).map { offset in
        alphabet[(start + offset * 7) % alphabet.count]
    })
}

private func sanitizeNamespace(_ raw: String) -> String {
    raw.isEmpty ? "default" : raw
}

private func makeFixtures() -> [ClusterFixture] {
    [
        ClusterFixture(
            contextName: "fake-orbit-mesh",
            defaultNamespace: "alpha-zone",
            namespaces: [
                NamespaceFixture(
                    name: "alpha-zone",
                    deploymentNames: [
                        "orbit-lens", "orbit-hopper", "quartz-ribbon", "quartz-pivot",
                        "ember-gate", "ember-flare", "sable-rivet",
                        "sable-arc", "lumen-kite", "lumen-thread"
                    ],
                    statefulSetNames: ["orbit-vault", "orbit-cache"],
                    daemonSetNames: ["alpha-node-shadow"],
                    cronJobNames: ["orbit-sweep-cycle", "quartz-rinse-loop"],
                    ingressTargets: ["orbit-lens", "ember-gate"],
                    configMapNames: ["orbit-grid", "ember-flags", "lumen-matrix"],
                    secretNames: ["orbit-seal", "ember-seal"],
                    networkPolicyNames: ["alpha-default-deny", "alpha-egress-lattice"],
                    roleNames: ["alpha-reader", "alpha-operator"],
                    roleBindingNames: ["alpha-reader-binding", "alpha-operator-binding"]
                ),
                NamespaceFixture(
                    name: "bravo-zone",
                    deploymentNames: [
                        "bravo-spoke", "bravo-echo", "cinder-pulse", "cinder-spindle",
                        "velvet-spring", "velvet-fold", "cobalt-drift"
                    ],
                    statefulSetNames: ["bravo-ledger"],
                    daemonSetNames: ["bravo-node-shadow"],
                    cronJobNames: ["cinder-rollup-cycle", "velvet-scrub-loop"],
                    ingressTargets: ["bravo-spoke"],
                    configMapNames: ["bravo-grid", "cinder-patterns"],
                    secretNames: ["bravo-seal", "cinder-seal"],
                    networkPolicyNames: ["bravo-default-deny"],
                    roleNames: ["bravo-reader"],
                    roleBindingNames: ["bravo-reader-binding"]
                ),
                NamespaceFixture(
                    name: "charlie-zone",
                    deploymentNames: [
                        "charlie-gate", "charlie-weave", "delta-knot", "delta-scan",
                        "murmur-check"
                    ],
                    statefulSetNames: [],
                    daemonSetNames: ["charlie-node-shadow"],
                    cronJobNames: ["delta-rotate-loop"],
                    ingressTargets: ["charlie-gate"],
                    configMapNames: ["charlie-routing", "delta-limits"],
                    secretNames: ["charlie-seal"],
                    networkPolicyNames: ["charlie-default-deny"],
                    roleNames: ["charlie-reader"],
                    roleBindingNames: ["charlie-reader-binding"]
                )
            ],
            nodes: [
                NodeFixture(name: "orbit-node-a", hostIP: "10.10.0.11", cpu: "410m", cpuPercent: "21%", memory: "1820Mi", memoryPercent: "44%"),
                NodeFixture(name: "orbit-node-b", hostIP: "10.10.0.12", cpu: "360m", cpuPercent: "18%", memory: "1710Mi", memoryPercent: "41%"),
                NodeFixture(name: "orbit-node-c", hostIP: "10.10.0.13", cpu: "390m", cpuPercent: "20%", memory: "1640Mi", memoryPercent: "39%")
            ]
        ),
        ClusterFixture(
            contextName: "fake-lattice-spark",
            defaultNamespace: "delta-zone",
            namespaces: [
                NamespaceFixture(
                    name: "delta-zone",
                    deploymentNames: [
                        "aurora-signal-weaver", "nebula-vector-engine", "quantum-echo-runner",
                        "ion-spline-buffer", "lumen-glyph-bridge", "radial-oxide-pump",
                        "sable-cascade-router", "prism-phase-lifter", "vertex-murmur-core",
                        "helix-bloom-driver", "cobalt-flare-spindle", "opal-drift-anchor",
                        "kinetic-mesh-orbit", "solstice-pulse-array", "ember-vault-cipher",
                        "atlas-fog-relay", "delta-spark-fabric", "axiom-wave-shaper",
                        "nimbus-thread-hopper", "fable-ridge-parser"
                    ],
                    statefulSetNames: ["delta-vault-core", "delta-cache-core"],
                    daemonSetNames: ["delta-node-shadow"],
                    cronJobNames: [
                        "midnight-echo-sweep", "gamma-spline-rollup",
                        "vector-prism-rinse", "lattice-bloom-sampler"
                    ],
                    ingressTargets: ["nebula-vector-engine", "ember-vault-cipher"],
                    configMapNames: ["delta-grid", "delta-synonyms", "delta-flags"],
                    secretNames: ["delta-seal", "vector-seal"],
                    networkPolicyNames: ["delta-default-deny", "delta-egress-lattice"],
                    roleNames: ["delta-reader", "delta-operator"],
                    roleBindingNames: ["delta-reader-binding", "delta-operator-binding"]
                ),
                NamespaceFixture(
                    name: "echo-zone",
                    deploymentNames: [
                        "echo-beacon", "echo-thread", "hollow-suggester", "hollow-speller",
                        "facet-lamp-warmer", "ranking-shift-loader"
                    ],
                    statefulSetNames: ["echo-vault-core"],
                    daemonSetNames: ["echo-node-shadow"],
                    cronJobNames: ["echo-reindex-loop"],
                    ingressTargets: ["echo-beacon"],
                    configMapNames: ["echo-grid", "echo-weights"],
                    secretNames: ["echo-seal"],
                    networkPolicyNames: ["echo-default-deny"],
                    roleNames: ["echo-reader"],
                    roleBindingNames: ["echo-reader-binding"]
                ),
                NamespaceFixture(
                    name: "foxtrot-zone",
                    deploymentNames: [
                        "foxtrot-orchestrator", "foxtrot-signal", "backfill-knot", "report-arc"
                    ],
                    statefulSetNames: [],
                    daemonSetNames: ["foxtrot-node-shadow"],
                    cronJobNames: ["nightly-knot-loop", "delta-replay-loop"],
                    ingressTargets: ["foxtrot-signal"],
                    configMapNames: ["foxtrot-grid"],
                    secretNames: ["foxtrot-seal"],
                    networkPolicyNames: ["foxtrot-default-deny"],
                    roleNames: ["foxtrot-reader"],
                    roleBindingNames: ["foxtrot-reader-binding"]
                )
            ],
            nodes: [
                NodeFixture(name: "lattice-node-a", hostIP: "10.20.0.21", cpu: "460m", cpuPercent: "24%", memory: "2140Mi", memoryPercent: "51%"),
                NodeFixture(name: "lattice-node-b", hostIP: "10.20.0.22", cpu: "430m", cpuPercent: "22%", memory: "2020Mi", memoryPercent: "48%"),
                NodeFixture(name: "lattice-node-c", hostIP: "10.20.0.23", cpu: "395m", cpuPercent: "20%", memory: "1870Mi", memoryPercent: "45%")
            ]
        )
    ]
}

private func fixture(named contextName: String) throws -> ClusterFixture {
    guard let fixture = makeFixtures().first(where: { $0.contextName == contextName }) else {
        throw CommandFailure(message: "Unknown fake context: \(contextName)")
    }
    return fixture
}

private func namespaceFixture(cluster: ClusterFixture, namespace: String) throws -> NamespaceFixture {
    let resolved = sanitizeNamespace(namespace)
    guard let fixture = cluster.namespaces.first(where: { $0.name == resolved }) else {
        throw CommandFailure(message: "Unknown namespace '\(resolved)' in context \(cluster.contextName)")
    }
    return fixture
}

private func deploymentReplicas(_ name: String) -> Int {
    2 + stableInt(name, modulo: 3)
}

private func statefulSetReplicas(_ name: String) -> Int {
    1 + stableInt(name, modulo: 2)
}

private func cronJobRunCount(_ name: String) -> Int {
    3 + stableInt(name, modulo: 2)
}

private func deploymentPods(cluster: ClusterFixture, namespace: NamespaceFixture, deploymentName: String) -> [PodFixture] {
    let hash = stableToken("\(namespace.name)/\(deploymentName)", length: 10)
    return (0..<deploymentReplicas(deploymentName)).map { ordinal in
        let suffix = stableToken("\(deploymentName)-\(ordinal)", length: 5)
        let node = cluster.nodes[(ordinal + stableInt(deploymentName, modulo: cluster.nodes.count)) % cluster.nodes.count]
        let phaseOptions = ["Running", "Running", "Running", "Pending"]
        let phase = phaseOptions[stableInt("\(deploymentName)-phase-\(ordinal)", modulo: phaseOptions.count)]
        let labels = [
            "app": deploymentName,
            "component": ordinal % 2 == 0 ? "api" : "worker",
            "tier": namespace.name
        ]
        let containers = ordinal % 2 == 0 ? [trimmedContainerName(deploymentName)] : [trimmedContainerName(deploymentName), "metrics"]
        return PodFixture(
            name: "\(deploymentName)-\(hash)-\(suffix)",
            namespace: namespace.name,
            workloadName: deploymentName,
            ownerKind: "ReplicaSet",
            phase: phase,
            restarts: stableInt("\(deploymentName)-restarts-\(ordinal)", modulo: 3),
            createdAt: timestamp(daysAgo: 11 - stableInt(deploymentName, modulo: 5), hoursAgo: ordinal * 3),
            cpu: "\(8 + stableInt("\(deploymentName)-cpu-\(ordinal)", modulo: 60))m",
            memory: "\(96 + stableInt("\(deploymentName)-mem-\(ordinal)", modulo: 220))Mi",
            nodeName: node.name,
            hostIP: node.hostIP,
            podIP: "10.\(20 + stableInt(namespace.name, modulo: 20)).\(40 + stableInt(deploymentName, modulo: 60)).\(10 + ordinal)",
            labels: labels,
            containers: containers
        )
    }
}

private func statefulSetPods(cluster: ClusterFixture, namespace: NamespaceFixture, statefulSetName: String) -> [PodFixture] {
    (0..<statefulSetReplicas(statefulSetName)).map { ordinal in
        let node = cluster.nodes[(ordinal + 1) % cluster.nodes.count]
        return PodFixture(
            name: "\(statefulSetName)-\(ordinal)",
            namespace: namespace.name,
            workloadName: statefulSetName,
            ownerKind: "StatefulSet",
            phase: "Running",
            restarts: stableInt("\(statefulSetName)-restart-\(ordinal)", modulo: 2),
            createdAt: timestamp(daysAgo: 14, hoursAgo: ordinal * 8),
            cpu: "\(12 + stableInt("\(statefulSetName)-cpu-\(ordinal)", modulo: 18))m",
            memory: "\(180 + stableInt("\(statefulSetName)-mem-\(ordinal)", modulo: 90))Mi",
            nodeName: node.name,
            hostIP: node.hostIP,
            podIP: "10.\(60 + stableInt(namespace.name, modulo: 20)).\(10 + stableInt(statefulSetName, modulo: 80)).\(40 + ordinal)",
            labels: [
                "app": statefulSetName,
                "statefulset.kubernetes.io/pod-name": "\(statefulSetName)-\(ordinal)"
            ],
            containers: [trimmedContainerName(statefulSetName)]
        )
    }
}

private func daemonSetPods(cluster: ClusterFixture, namespace: NamespaceFixture, daemonSetName: String) -> [PodFixture] {
    cluster.nodes.enumerated().map { index, node in
        PodFixture(
            name: "\(daemonSetName)-\(stableToken("\(daemonSetName)-\(node.name)", length: 5))",
            namespace: namespace.name,
            workloadName: daemonSetName,
            ownerKind: "DaemonSet",
            phase: "Running",
            restarts: stableInt("\(daemonSetName)-restart-\(index)", modulo: 2),
            createdAt: timestamp(daysAgo: 13, hoursAgo: index * 4),
            cpu: "\(5 + stableInt("\(daemonSetName)-cpu-\(index)", modulo: 12))m",
            memory: "\(64 + stableInt("\(daemonSetName)-mem-\(index)", modulo: 48))Mi",
            nodeName: node.name,
            hostIP: node.hostIP,
            podIP: "10.\(80 + stableInt(namespace.name, modulo: 20)).\(index + 1).\(30 + index)",
            labels: [
                "app": daemonSetName,
                "k8s-app": daemonSetName
            ],
            containers: [trimmedContainerName(daemonSetName)]
        )
    }
}

private func cronJobPods(cluster: ClusterFixture, namespace: NamespaceFixture, cronJobName: String) -> [PodFixture] {
    (0..<cronJobRunCount(cronJobName)).map { ordinal in
        let run = 29_600_000 + stableInt("\(cronJobName)-run-\(ordinal)", modulo: 200_000)
        let node = cluster.nodes[(ordinal + 2) % cluster.nodes.count]
        return PodFixture(
            name: "\(cronJobName)-\(run)-\(stableToken("\(cronJobName)-\(ordinal)", length: 5))",
            namespace: namespace.name,
            workloadName: cronJobName,
            ownerKind: "Job",
            phase: "Succeeded",
            restarts: 0,
            createdAt: timestamp(daysAgo: ordinal, hoursAgo: 2 + ordinal),
            cpu: "0m",
            memory: "0Mi",
            nodeName: node.name,
            hostIP: node.hostIP,
            podIP: "10.\(100 + stableInt(namespace.name, modulo: 10)).\(stableInt(cronJobName, modulo: 50)).\(20 + ordinal)",
            labels: [
                "job-name": "\(cronJobName)-\(run)",
                "cronjob": cronJobName
            ],
            containers: [trimmedContainerName(cronJobName)]
        )
    }
}

private func pods(cluster: ClusterFixture, namespace: NamespaceFixture) -> [PodFixture] {
    let deploymentRows = namespace.deploymentNames.flatMap { deploymentPods(cluster: cluster, namespace: namespace, deploymentName: $0) }
    let statefulRows = namespace.statefulSetNames.flatMap { statefulSetPods(cluster: cluster, namespace: namespace, statefulSetName: $0) }
    let daemonRows = namespace.daemonSetNames.flatMap { daemonSetPods(cluster: cluster, namespace: namespace, daemonSetName: $0) }
    let cronJobRows = namespace.cronJobNames.flatMap { cronJobPods(cluster: cluster, namespace: namespace, cronJobName: $0) }
    return (deploymentRows + statefulRows + daemonRows + cronJobRows)
        .sorted { $0.name < $1.name }
}

private func allPods(cluster: ClusterFixture) -> [PodFixture] {
    cluster.namespaces.flatMap { pods(cluster: cluster, namespace: $0) }
}

private func trimmedContainerName(_ base: String) -> String {
    base
        .split(separator: "-")
        .last
        .map(String.init) ?? "app"
}

private func podObject(_ pod: PodFixture) -> [String: Any] {
    let containerObjects = pod.containers.map { containerName in
        [
            "name": containerName,
            "image": "ghcr.io/rune/\(pod.workloadName):2026.04.\(stableInt(containerName, modulo: 12) + 1)"
        ]
    }
    let state: [String: Any]
    switch pod.phase {
    case "Succeeded":
        state = ["terminated": ["exitCode": 0, "reason": "Completed", "finishedAt": pod.createdAt]]
    case "Pending":
        state = ["waiting": ["reason": "ContainerCreating"]]
    default:
        state = ["running": ["startedAt": pod.createdAt]]
    }
    return [
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": [
            "name": pod.name,
            "namespace": pod.namespace,
            "uid": "uid-\(stableToken(pod.name, length: 8))",
            "creationTimestamp": pod.createdAt,
            "labels": pod.labels,
            "ownerReferences": [[
                "apiVersion": "apps/v1",
                "kind": pod.ownerKind,
                "name": pod.ownerKind == "ReplicaSet" ? "\(pod.workloadName)-\(stableToken(pod.workloadName, length: 10))" : pod.workloadName
            ]]
        ],
        "spec": [
            "nodeName": pod.nodeName,
            "containers": containerObjects
        ],
        "status": [
            "phase": pod.phase,
            "hostIP": pod.hostIP,
            "podIP": pod.podIP,
            "startTime": pod.createdAt,
            "qosClass": pod.phase == "Succeeded" ? "Burstable" : "Guaranteed",
            "containerStatuses": pod.containers.map { containerName in
                [
                    "name": containerName,
                    "ready": pod.phase == "Running",
                    "restartCount": pod.restarts,
                    "state": state
                ]
            }
        ]
    ]
}

private func deploymentObject(namespace: NamespaceFixture, deploymentName: String) -> [String: Any] {
    let replicas = deploymentReplicas(deploymentName)
    return [
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": [
            "name": deploymentName,
            "namespace": namespace.name,
            "creationTimestamp": timestamp(daysAgo: 14, hoursAgo: stableInt(deploymentName, modulo: 24))
        ],
        "spec": [
            "replicas": replicas,
            "selector": [
                "matchLabels": ["app": deploymentName]
            ],
            "template": [
                "metadata": ["labels": ["app": deploymentName]],
                "spec": [
                    "containers": [[
                        "name": trimmedContainerName(deploymentName),
                        "image": "ghcr.io/rune/\(deploymentName):2026.04.\(stableInt(deploymentName, modulo: 12) + 1)"
                    ]]
                ]
            ]
        ],
        "status": [
            "replicas": replicas,
            "updatedReplicas": replicas,
            "readyReplicas": max(1, replicas - stableInt(deploymentName, modulo: 2)),
            "availableReplicas": max(1, replicas - stableInt("\(deploymentName)-available", modulo: 2))
        ]
    ]
}

private func serviceObject(namespace: NamespaceFixture, serviceName: String) -> [String: Any] {
    [
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": [
            "name": serviceName,
            "namespace": namespace.name,
            "creationTimestamp": timestamp(daysAgo: 12, hoursAgo: stableInt(serviceName, modulo: 24))
        ],
        "spec": [
            "type": "ClusterIP",
            "clusterIP": "10.96.\(stableInt(namespace.name, modulo: 60)).\(20 + stableInt(serviceName, modulo: 200))",
            "selector": ["app": serviceName],
            "ports": [[
                "name": "http",
                "port": 80,
                "targetPort": 8080
            ]]
        ]
    ]
}

private func statefulSetObject(namespace: NamespaceFixture, name: String) -> [String: Any] {
    let replicas = statefulSetReplicas(name)
    return [
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": [
            "name": name,
            "namespace": namespace.name
        ],
        "spec": [
            "serviceName": name,
            "replicas": replicas
        ],
        "status": [
            "replicas": replicas,
            "readyReplicas": replicas
        ]
    ]
}

private func daemonSetObject(namespace: NamespaceFixture, cluster: ClusterFixture, name: String) -> [String: Any] {
    [
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": [
            "name": name,
            "namespace": namespace.name
        ],
        "status": [
            "desiredNumberScheduled": cluster.nodes.count,
            "currentNumberScheduled": cluster.nodes.count,
            "numberReady": cluster.nodes.count
        ]
    ]
}

private func cronJobObject(namespace: NamespaceFixture, name: String) -> [String: Any] {
    [
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": [
            "name": name,
            "namespace": namespace.name
        ],
        "spec": [
            "schedule": stableInt(name, modulo: 2) == 0 ? "*/20 * * * *" : "0 */6 * * *",
            "suspend": false
        ],
        "status": [
            "lastScheduleTime": timestamp(daysAgo: 0, hoursAgo: 1 + stableInt(name, modulo: 4))
        ]
    ]
}

private func jobs(namespace: NamespaceFixture) -> [[String: Any]] {
    namespace.cronJobNames.flatMap { cronJobName in
        (0..<cronJobRunCount(cronJobName)).map { ordinal in
            let run = 29_600_000 + stableInt("\(cronJobName)-run-\(ordinal)", modulo: 200_000)
            return [
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": [
                    "name": "\(cronJobName)-\(run)",
                    "namespace": namespace.name
                ],
                "status": [
                    "succeeded": 1,
                    "active": 0,
                    "failed": 0
                ]
            ]
        }
    }
}

private func replicaSets(namespace: NamespaceFixture) -> [[String: Any]] {
    namespace.deploymentNames.map { deploymentName in
        [
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": [
                "name": "\(deploymentName)-\(stableToken(deploymentName, length: 10))",
                "namespace": namespace.name
            ],
            "spec": [
                "replicas": deploymentReplicas(deploymentName)
            ],
            "status": [
                "replicas": deploymentReplicas(deploymentName),
                "readyReplicas": max(1, deploymentReplicas(deploymentName) - 1)
            ]
        ]
    }
}

private func configMapObject(namespace: NamespaceFixture, name: String) -> [String: Any] {
    [
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": [
            "name": name,
            "namespace": namespace.name
        ],
        "data": [
            "LOG_LEVEL": stableInt(name, modulo: 2) == 0 ? "info" : "debug",
            "FEATURE_FLAG": stableInt(name, modulo: 2) == 0 ? "true" : "false",
            "OWNER": namespace.name
        ]
    ]
}

private func secretObject(namespace: NamespaceFixture, name: String) -> [String: Any] {
    [
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": [
            "name": name,
            "namespace": namespace.name
        ],
        "type": "Opaque",
        "data": [
            "username": "cnVuZQ==",
            "password": "ZmFrZS1zZWNyZXQ="
        ]
    ]
}

private func ingressObject(namespace: NamespaceFixture, serviceName: String) -> [String: Any] {
    [
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": [
            "name": "\(serviceName)-public",
            "namespace": namespace.name
        ],
        "spec": [
            "rules": [[
                "host": "\(serviceName).\(namespace.name).fake.rune.local",
                "http": [
                    "paths": [[
                        "path": "/",
                        "pathType": "Prefix",
                        "backend": [
                            "service": [
                                "name": serviceName,
                                "port": ["number": 80]
                            ]
                        ]
                    ]]
                ]
            ]]
        ],
        "status": [
            "loadBalancer": [
                "ingress": [[
                    "hostname": "lb-\(stableToken(serviceName, length: 6)).fake.rune.local"
                ]]
            ]
        ]
    ]
}

private func pvcObject(namespace: NamespaceFixture, name: String) -> [String: Any] {
    [
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": [
            "name": "\(name)-data",
            "namespace": namespace.name
        ],
        "spec": [
            "resources": [
                "requests": ["storage": "20Gi"]
            ]
        ],
        "status": [
            "phase": "Bound",
            "capacity": ["storage": "20Gi"]
        ]
    ]
}

private func hpaObject(namespace: NamespaceFixture, deploymentName: String) -> [String: Any] {
    [
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": [
            "name": "\(deploymentName)-hpa",
            "namespace": namespace.name
        ],
        "spec": [
            "minReplicas": 2,
            "maxReplicas": 8,
            "scaleTargetRef": [
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": deploymentName
            ]
        ],
        "status": [
            "currentReplicas": min(4, deploymentReplicas(deploymentName)),
            "desiredReplicas": deploymentReplicas(deploymentName)
        ]
    ]
}

private func networkPolicyObject(namespace: NamespaceFixture, name: String) -> [String: Any] {
    [
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": [
            "name": name,
            "namespace": namespace.name
        ],
        "spec": [
            "podSelector": [:],
            "policyTypes": ["Ingress", "Egress"]
        ]
    ]
}

private func roleObject(namespace: NamespaceFixture, name: String) -> [String: Any] {
    [
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": [
            "name": name,
            "namespace": namespace.name
        ],
        "rules": [[
            "apiGroups": [""],
            "resources": ["pods", "services", "configmaps"],
            "verbs": ["get", "list", "watch"]
        ]]
    ]
}

private func roleBindingObject(namespace: NamespaceFixture, name: String) -> [String: Any] {
    let roleName = name.replacingOccurrences(of: "-binding", with: "")
    return [
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": [
            "name": name,
            "namespace": namespace.name
        ],
        "subjects": [[
            "kind": "ServiceAccount",
            "name": "\(namespace.name)-runner",
            "namespace": namespace.name
        ]],
        "roleRef": [
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": roleName
        ]
    ]
}

private func nodeObject(_ node: NodeFixture) -> [String: Any] {
    [
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": [
            "name": node.name
        ],
        "status": [
            "addresses": [[
                "type": "InternalIP",
                "address": node.hostIP
            ]],
            "conditions": [[
                "type": "Ready",
                "status": "True"
            ]],
            "nodeInfo": [
                "kubeletVersion": "v1.31.2",
                "containerRuntimeVersion": "containerd://2.0.4"
            ]
        ]
    ]
}

private func events(cluster: ClusterFixture, namespace: NamespaceFixture) -> [[String: Any]] {
    Array(pods(cluster: cluster, namespace: namespace).prefix(30).enumerated()).map { index, pod in
        let reasons = ["Scheduled", "Pulled", "Created", "Started", "BackOff"]
        let reason = reasons[index % reasons.count]
        let type = reason == "BackOff" ? "Warning" : "Normal"
        return [
            "apiVersion": "v1",
            "kind": "Event",
            "metadata": [
                "name": "\(pod.name).\(stableToken("\(pod.name)-event", length: 6))",
                "namespace": namespace.name
            ],
            "type": type,
            "reason": reason,
            "message": type == "Warning"
                ? "Back-off restarting failed container \(pod.containers.first ?? "app") in pod \(pod.name)"
                : "Pod \(pod.name) transitioned to \(pod.phase)",
            "lastTimestamp": timestamp(daysAgo: 0, hoursAgo: index / 4, minutesAgo: index * 2),
            "involvedObject": [
                "kind": "Pod",
                "name": pod.name,
                "namespace": namespace.name
            ]
        ]
    }
}

private func clusterRoles() -> [[String: Any]] {
    ["cluster-reader", "cluster-debugger", "cluster-operator"].map { name in
        [
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": ["name": name]
        ]
    }
}

private func clusterRoleBindings() -> [[String: Any]] {
    ["cluster-reader-binding", "cluster-debugger-binding"].map { name in
        [
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "metadata": ["name": name]
        ]
    }
}

private func storageClasses() -> [[String: Any]] {
    [
        [
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": [
                "name": "fast-ssd",
                "annotations": ["storageclass.kubernetes.io/is-default-class": "true"]
            ],
            "provisioner": "kubernetes.io/no-provisioner"
        ],
        [
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": ["name": "bulk-hdd"],
            "provisioner": "kubernetes.io/no-provisioner"
        ]
    ]
}

private func resourceList(cluster: ClusterFixture, namespace: NamespaceFixture?, resource: String, allNamespaces: Bool) throws -> [[String: Any]] {
    switch normalizedResourceName(resource) {
    case "pods":
        if allNamespaces { return allPods(cluster: cluster).map(podObject) }
        return pods(cluster: cluster, namespace: try required(namespace)).map(podObject)
    case "deployments":
        if allNamespaces {
            return cluster.namespaces.flatMap { ns in ns.deploymentNames.map { deploymentObject(namespace: ns, deploymentName: $0) } }
        }
        return try required(namespace).deploymentNames.map { deploymentObject(namespace: try required(namespace), deploymentName: $0) }
    case "statefulsets":
        if allNamespaces {
            return cluster.namespaces.flatMap { ns in ns.statefulSetNames.map { statefulSetObject(namespace: ns, name: $0) } }
        }
        return try required(namespace).statefulSetNames.map { statefulSetObject(namespace: try required(namespace), name: $0) }
    case "daemonsets":
        if allNamespaces {
            return cluster.namespaces.flatMap { ns in ns.daemonSetNames.map { daemonSetObject(namespace: ns, cluster: cluster, name: $0) } }
        }
        return try required(namespace).daemonSetNames.map { daemonSetObject(namespace: try required(namespace), cluster: cluster, name: $0) }
    case "jobs":
        if allNamespaces { return cluster.namespaces.flatMap(jobs(namespace:)) }
        return jobs(namespace: try required(namespace))
    case "cronjobs":
        if allNamespaces {
            return cluster.namespaces.flatMap { ns in ns.cronJobNames.map { cronJobObject(namespace: ns, name: $0) } }
        }
        return try required(namespace).cronJobNames.map { cronJobObject(namespace: try required(namespace), name: $0) }
    case "replicasets":
        if allNamespaces { return cluster.namespaces.flatMap(replicaSets(namespace:)) }
        return replicaSets(namespace: try required(namespace))
    case "services":
        if allNamespaces {
            return cluster.namespaces.flatMap { ns in (ns.deploymentNames + ns.statefulSetNames).map { serviceObject(namespace: ns, serviceName: $0) } }
        }
        let ns = try required(namespace)
        return (ns.deploymentNames + ns.statefulSetNames).map { serviceObject(namespace: ns, serviceName: $0) }
    case "ingresses":
        if allNamespaces {
            return cluster.namespaces.flatMap { ns in ns.ingressTargets.map { ingressObject(namespace: ns, serviceName: $0) } }
        }
        return try required(namespace).ingressTargets.map { ingressObject(namespace: try required(namespace), serviceName: $0) }
    case "configmaps":
        if allNamespaces {
            return cluster.namespaces.flatMap { ns in ns.configMapNames.map { configMapObject(namespace: ns, name: $0) } }
        }
        return try required(namespace).configMapNames.map { configMapObject(namespace: try required(namespace), name: $0) }
    case "secrets":
        if allNamespaces {
            return cluster.namespaces.flatMap { ns in ns.secretNames.map { secretObject(namespace: ns, name: $0) } }
        }
        return try required(namespace).secretNames.map { secretObject(namespace: try required(namespace), name: $0) }
    case "events":
        if allNamespaces {
            return cluster.namespaces.flatMap { ns in events(cluster: cluster, namespace: ns) }
        }
        return events(cluster: cluster, namespace: try required(namespace))
    case "pvc":
        if allNamespaces {
            return cluster.namespaces.flatMap { ns in ns.statefulSetNames.map { pvcObject(namespace: ns, name: $0) } }
        }
        return try required(namespace).statefulSetNames.map { pvcObject(namespace: try required(namespace), name: $0) }
    case "pv":
        return cluster.namespaces.flatMap { ns in
            ns.statefulSetNames.map { name in
                [
                    "apiVersion": "v1",
                    "kind": "PersistentVolume",
                    "metadata": ["name": "\(name)-pv"],
                    "spec": ["capacity": ["storage": "20Gi"]],
                    "status": ["phase": "Bound"]
                ]
            }
        }
    case "storageclass":
        return storageClasses()
    case "hpa":
        if allNamespaces {
            return cluster.namespaces.flatMap { ns in ns.deploymentNames.prefix(3).map { hpaObject(namespace: ns, deploymentName: $0) } }
        }
        return try required(namespace).deploymentNames.prefix(3).map { hpaObject(namespace: try required(namespace), deploymentName: $0) }
    case "networkpolicy":
        if allNamespaces {
            return cluster.namespaces.flatMap { ns in ns.networkPolicyNames.map { networkPolicyObject(namespace: ns, name: $0) } }
        }
        return try required(namespace).networkPolicyNames.map { networkPolicyObject(namespace: try required(namespace), name: $0) }
    case "nodes":
        return cluster.nodes.map(nodeObject)
    case "namespaces":
        return cluster.namespaces.map { ["metadata": ["name": $0.name]] }
    case "roles":
        return try required(namespace).roleNames.map { roleObject(namespace: try required(namespace), name: $0) }
    case "rolebindings":
        return try required(namespace).roleBindingNames.map { roleBindingObject(namespace: try required(namespace), name: $0) }
    case "clusterroles":
        return clusterRoles()
    case "clusterrolebindings":
        return clusterRoleBindings()
    default:
        throw CommandFailure(message: "Unsupported fake resource: \(resource)")
    }
}

private func required(_ namespace: NamespaceFixture?) throws -> NamespaceFixture {
    guard let namespace else {
        throw CommandFailure(message: "This fake resource requires a namespace")
    }
    return namespace
}

private func normalizedResourceName(_ resource: String) -> String {
    switch resource {
    case "po", "pod", "pods": return "pods"
    case "deploy", "deployment", "deployments": return "deployments"
    case "sts", "statefulset", "statefulsets": return "statefulsets"
    case "ds", "daemonset", "daemonsets": return "daemonsets"
    case "job", "jobs": return "jobs"
    case "cronjob", "cronjobs": return "cronjobs"
    case "rs", "replicaset", "replicasets": return "replicasets"
    case "svc", "service", "services": return "services"
    case "ing", "ingress", "ingresses": return "ingresses"
    case "cm", "configmap", "configmaps": return "configmaps"
    case "secret", "secrets": return "secrets"
    case "events": return "events"
    case "pvc": return "pvc"
    case "pv": return "pv"
    case "storageclass", "storageclasses": return "storageclass"
    case "hpa": return "hpa"
    case "networkpolicy", "networkpolicies": return "networkpolicy"
    case "node", "nodes": return "nodes"
    case "namespace", "namespaces": return "namespaces"
    case "role", "roles": return "roles"
    case "rolebinding", "rolebindings": return "rolebindings"
    case "clusterrole", "clusterroles": return "clusterroles"
    case "clusterrolebinding", "clusterrolebindings": return "clusterrolebindings"
    default: return resource
    }
}

private func objectByName(cluster: ClusterFixture, namespace: NamespaceFixture?, resource: String, name: String) throws -> [String: Any] {
    let items = try resourceList(cluster: cluster, namespace: namespace, resource: resource, allNamespaces: false)
    guard let match = items.first(where: { (($0["metadata"] as? [String: Any])?["name"] as? String) == name }) else {
        throw CommandFailure(message: "No fake \(resource) named \(name)")
    }
    return match
}

private func jsonString(_ object: Any) throws -> String {
    let data = try JSONSerialization.data(withJSONObject: object, options: [.prettyPrinted, .sortedKeys])
    guard let string = String(data: data, encoding: .utf8) else {
        throw CommandFailure(message: "Failed to encode JSON output")
    }
    return string + "\n"
}

private func yamlString(_ value: Any, indent: Int = 0) -> String {
    let prefix = String(repeating: " ", count: indent)
    if let dictionary = value as? [String: Any] {
        return dictionary.keys.sorted().map { key in
            let item = dictionary[key] as Any
            if item is [String: Any] || item is [Any] {
                return "\(prefix)\(key):\n" + yamlString(item, indent: indent + 2)
            }
            return "\(prefix)\(key): \(yamlScalar(item))"
        }.joined(separator: "\n")
    }
    if let array = value as? [Any] {
        return array.map { item in
            if item is [String: Any] || item is [Any] {
                return "\(prefix)-\n" + yamlString(item, indent: indent + 2)
            }
            return "\(prefix)- \(yamlScalar(item))"
        }.joined(separator: "\n")
    }
    return "\(prefix)\(yamlScalar(value))"
}

private func yamlScalar(_ value: Any) -> String {
    switch value {
    case let string as String:
        if string.isEmpty { return "\"\"" }
        if string.contains(":") || string.contains("#") || string.contains(" ") {
            return "\"\(string)\""
        }
        return string
    case let number as NSNumber:
        return number.stringValue
    case _ as NSNull:
        return "null"
    default:
        return "\"\(value)\""
    }
}

private func podLogs(_ pod: PodFixture, previous: Bool) -> String {
    if previous {
        return "No previous logs available for \(pod.name).\n"
    }
    let level = pod.phase == "Pending" ? "WARN" : "INFO"
    let container = pod.containers.first ?? "app"
    let lines = (0..<60).map { index in
        let ts = timestamp(daysAgo: 0, hoursAgo: 0, minutesAgo: max(0, 59 - index))
        return "\(ts) \(level) \(container) pod=\(pod.name) ctx=\(pod.namespace) step=\(index) synthetic fake-k8s log line"
    }
    return lines.joined(separator: "\n") + "\n"
}

private func describeObject(_ object: [String: Any]) -> String {
    let metadata = object["metadata"] as? [String: Any] ?? [:]
    let spec = object["spec"] as? [String: Any] ?? [:]
    let status = object["status"] as? [String: Any] ?? [:]
    let kind = object["kind"] as? String ?? "Resource"
    let name = metadata["name"] as? String ?? "unknown"
    let namespace = metadata["namespace"] as? String ?? "cluster"

    var lines: [String] = []
    lines.append("Name:           \(name)")
    lines.append("Namespace:      \(namespace)")
    lines.append("Kind:           \(kind)")
    if let created = metadata["creationTimestamp"] as? String {
        lines.append("Created At:     \(created)")
    }
    if let selector = (spec["selector"] as? [String: Any])?["matchLabels"] as? [String: String] {
        lines.append("Selector:       \(selector.map { "\($0.key)=\($0.value)" }.sorted().joined(separator: ","))")
    } else if let selector = spec["selector"] as? [String: String] {
        lines.append("Selector:       \(selector.map { "\($0.key)=\($0.value)" }.sorted().joined(separator: ","))")
    }
    if let replicas = spec["replicas"] {
        lines.append("Replicas:       \(replicas)")
    }
    if let ready = status["readyReplicas"] {
        lines.append("Ready:          \(ready)")
    }
    if let phase = status["phase"] {
        lines.append("Phase:          \(phase)")
    }
    if let podIP = status["podIP"] {
        lines.append("Pod IP:         \(podIP)")
    }
    if let hostIP = status["hostIP"] {
        lines.append("Host IP:        \(hostIP)")
    }
    lines.append("")
    lines.append("Description:")
    lines.append("  Synthetic fake-k8s fixture for Rune layout debugging.")
    return lines.joined(separator: "\n") + "\n"
}

private func write(_ string: String) {
    if let data = string.data(using: .utf8) {
        FileHandle.standardOutput.write(data)
    }
}

private func fail(_ string: String) -> Never {
    if let data = (string + "\n").data(using: .utf8) {
        FileHandle.standardError.write(data)
    }
    Foundation.exit(1)
}

private func parseOption(_ args: [String], name: String) -> (String?, [String]) {
    var copy = args
    guard let index = copy.firstIndex(of: name), index + 1 < copy.count else {
        return (nil, args)
    }
    let value = copy[index + 1]
    copy.removeSubrange(index...(index + 1))
    return (value, copy)
}

private func globalContext(from args: [String]) -> (String?, [String]) {
    var filtered: [String] = []
    var context: String?
    var index = 0
    while index < args.count {
        let arg = args[index]
        if arg == "--context", index + 1 < args.count {
            context = args[index + 1]
            index += 2
            continue
        }
        if arg == "--request-timeout", index + 1 < args.count {
            index += 2
            continue
        }
        if arg.hasPrefix("--request-timeout=") || arg.hasPrefix("--chunk-size=") {
            index += 1
            continue
        }
        filtered.append(arg)
        index += 1
    }
    return (context, filtered)
}

private func flagValue(_ args: [String], short: String, long: String? = nil) -> String? {
    for (index, arg) in args.enumerated() {
        if arg == short || (long != nil && arg == long), index + 1 < args.count {
            return args[index + 1]
        }
        if let long, arg.hasPrefix("\(long)=") {
            return String(arg.dropFirst(long.count + 1))
        }
        if arg.hasPrefix("\(short)=") {
            return String(arg.dropFirst(short.count + 1))
        }
    }
    return nil
}

private func hasFlag(_ args: [String], short: String, long: String? = nil) -> Bool {
    args.contains(short) || (long != nil && args.contains(long!))
}

private func renderCustomColumns(resource: String, items: [[String: Any]], columns: String) -> String {
    let normalized = normalizedResourceName(resource)
    switch normalized {
    case "pods":
        if columns.contains("NAMESPACE:.metadata.namespace") {
            return items.compactMap { item in
                let metadata = item["metadata"] as? [String: Any] ?? [:]
                let status = item["status"] as? [String: Any] ?? [:]
                let name = metadata["name"] as? String ?? "pod"
                let namespace = metadata["namespace"] as? String ?? "default"
                let phase = status["phase"] as? String ?? "Unknown"
                let containerStatuses = status["containerStatuses"] as? [[String: Any]] ?? []
                let restarts = containerStatuses.reduce(0) { partial, row in
                    partial + ((row["restartCount"] as? Int) ?? 0)
                }
                let created = metadata["creationTimestamp"] as? String ?? "-"
                return "\(namespace)\t\(name)\t\(phase)\t\(restarts)\t\(created)"
            }.joined(separator: "\n") + "\n"
        }
        if columns.contains("RESTARTS:.status.containerStatuses[*].restartCount") {
            return items.compactMap { item in
                let metadata = item["metadata"] as? [String: Any] ?? [:]
                let status = item["status"] as? [String: Any] ?? [:]
                let name = metadata["name"] as? String ?? "pod"
                let phase = status["phase"] as? String ?? "Unknown"
                let containerStatuses = status["containerStatuses"] as? [[String: Any]] ?? []
                let restarts = containerStatuses.reduce(0) { partial, row in
                    partial + ((row["restartCount"] as? Int) ?? 0)
                }
                let created = metadata["creationTimestamp"] as? String ?? "-"
                return "\(name)\t\(phase)\t\(restarts)\t\(created)"
            }.joined(separator: "\n") + "\n"
        }
        return items.compactMap { item in
            let metadata = item["metadata"] as? [String: Any] ?? [:]
            let status = item["status"] as? [String: Any] ?? [:]
            return "\(metadata["name"] as? String ?? "pod")\t\(status["phase"] as? String ?? "Unknown")"
        }.joined(separator: "\n") + "\n"
    case "deployments":
        if columns.contains("NAMESPACE:.metadata.namespace") {
            return items.compactMap { item in
                let metadata = item["metadata"] as? [String: Any] ?? [:]
                let status = item["status"] as? [String: Any] ?? [:]
                let spec = item["spec"] as? [String: Any] ?? [:]
                return "\(metadata["namespace"] as? String ?? "default")\t\(metadata["name"] as? String ?? "deployment")\t\(status["readyReplicas"] as? Int ?? 0)\t\(spec["replicas"] as? Int ?? 0)"
            }.joined(separator: "\n") + "\n"
        }
        return items.compactMap { item in
            let metadata = item["metadata"] as? [String: Any] ?? [:]
            let status = item["status"] as? [String: Any] ?? [:]
            let spec = item["spec"] as? [String: Any] ?? [:]
            return "\(metadata["name"] as? String ?? "deployment")\t\(status["readyReplicas"] as? Int ?? 0)\t\(spec["replicas"] as? Int ?? 0)"
        }.joined(separator: "\n") + "\n"
    case "jobs":
        return items.compactMap { item in
            let metadata = item["metadata"] as? [String: Any] ?? [:]
            let status = item["status"] as? [String: Any] ?? [:]
            return "\(metadata["name"] as? String ?? "job")\t\(status["succeeded"] as? Int ?? 0)\t\(status["active"] as? Int ?? 0)\t\(status["failed"] as? Int ?? 0)"
        }.joined(separator: "\n") + "\n"
    case "cronjobs":
        return items.compactMap { item in
            let metadata = item["metadata"] as? [String: Any] ?? [:]
            let spec = item["spec"] as? [String: Any] ?? [:]
            return "\(metadata["name"] as? String ?? "cronjob")\t\(spec["schedule"] as? String ?? "*/30 * * * *")\t\(spec["suspend"] as? Bool ?? false)"
        }.joined(separator: "\n") + "\n"
    default:
        return items.compactMap { item in
            let metadata = item["metadata"] as? [String: Any] ?? [:]
            return metadata["name"] as? String
        }.joined(separator: "\n") + (items.isEmpty ? "" : "\n")
    }
}

private func renderTopPods(cluster: ClusterFixture, namespace: NamespaceFixture?, allNamespaces: Bool) -> String {
    let rows = allNamespaces
        ? allPods(cluster: cluster)
        : pods(cluster: cluster, namespace: namespace!)
    return rows
        .filter { $0.phase == "Running" }
        .map { pod in
            allNamespaces
                ? "\(pod.namespace)\t\(pod.name)\t\(pod.cpu)\t\(pod.memory)"
                : "\(pod.name)\t\(pod.cpu)\t\(pod.memory)"
        }
        .joined(separator: "\n") + "\n"
}

private func renderTopNodes(cluster: ClusterFixture) -> String {
    cluster.nodes.map { node in
        "\(node.name)\t\(node.cpu)\t\(node.cpuPercent)\t\(node.memory)\t\(node.memoryPercent)"
    }.joined(separator: "\n") + "\n"
}

private func renderRawProbe(cluster: ClusterFixture, context: String, apiPath: String) throws -> String {
    let parts = apiPath.split(separator: "?").first?.split(separator: "/").map(String.init) ?? []
    guard let resource = parts.last else {
        throw CommandFailure(message: "Unsupported raw path: \(apiPath)")
    }
    let namespaceName: String?
    if let namespaceIndex = parts.firstIndex(of: "namespaces"), namespaceIndex + 1 < parts.count {
        namespaceName = parts[namespaceIndex + 1]
    } else {
        namespaceName = nil
    }
    let clusterFixture = try fixture(named: context)
    let ns = try namespaceName.map { try namespaceFixture(cluster: clusterFixture, namespace: $0) }
    let items = try resourceList(cluster: clusterFixture, namespace: ns, resource: resource, allNamespaces: namespaceName == nil && resource == "pods" ? false : false)
    let remaining = max(0, items.count - 1)
    let probe: [String: Any] = [
        "kind": "List",
        "apiVersion": apiPath.contains("/apis/") ? "apps/v1" : "v1",
        "metadata": ["remainingItemCount": remaining],
        "items": items.isEmpty ? [] : [items[0]]
    ]
    return try jsonString(probe)
}

private func renderConfig(context: String?) throws -> String {
    let fixtures = makeFixtures()
    let chosen = context ?? fixtures.first?.contextName ?? "fake-orbit-mesh"
    let payload: [String: Any] = [
        "apiVersion": "v1",
        "kind": "Config",
        "current-context": chosen,
        "clusters": fixtures.map { fixture in
            [
                "name": fixture.contextName,
                "cluster": ["server": "https://\(fixture.contextName).fake.rune.local"]
            ]
        },
        "contexts": fixtures.map { fixture in
            [
                "name": fixture.contextName,
                "context": [
                    "cluster": fixture.contextName,
                    "namespace": fixture.defaultNamespace,
                    "user": "fake-user"
                ]
            ]
        },
        "users": [[
            "name": "fake-user",
            "user": ["token": "fake-token"]
        ]]
    ]
    return try jsonString(payload)
}

private func kubeconfigYAML(stateDir: URL) -> String {
    let contexts = makeFixtures()
    let clusters = contexts.map { fixture in
        """
        - name: \(fixture.contextName)
          cluster:
            server: https://\(fixture.contextName).fake.rune.local
        """
    }.joined(separator: "\n")
    let contextRows = contexts.map { fixture in
        """
        - name: \(fixture.contextName)
          context:
            cluster: \(fixture.contextName)
            namespace: \(fixture.defaultNamespace)
            user: fake-user
        """
    }.joined(separator: "\n")
    return """
    apiVersion: v1
    kind: Config
    current-context: \(contexts.first?.contextName ?? "fake-orbit-mesh")
    preferences: {}
    clusters:
    \(clusters)
    contexts:
    \(contextRows)
    users:
    - name: fake-user
      user:
        token: fake-token
    """
}

private func ensureSetup(stateDir: URL, binaryPath: String) throws {
    let fileManager = FileManager.default
    try fileManager.createDirectory(at: stateDir, withIntermediateDirectories: true)

    let kubeconfig = stateDir.appendingPathComponent("kubeconfig.yaml")
    try kubeconfigYAML(stateDir: stateDir).write(to: kubeconfig, atomically: true, encoding: .utf8)

    let envFile = """
    export KUBECONFIG="\(kubeconfig.path)"
    export RUNE_K8S_AGENT=""
    export RUNE_FAKE_K8S_STATE="\(stateDir.path)"
    export RUNE_FAKE_K8S_BINARY="\(binaryPath)"
    """
    try envFile.write(to: stateDir.appendingPathComponent("env.sh"), atomically: true, encoding: .utf8)
}

private func ensureRESTSetup(stateDir: URL, binaryPath: String, serverURL: String) throws {
    let fileManager = FileManager.default
    try fileManager.createDirectory(at: stateDir, withIntermediateDirectories: true)

    let kubeconfig = stateDir.appendingPathComponent("kubeconfig.yaml")
    let yaml = RuneFakeK8sKubeconfig.render(serverURL: serverURL)
    try yaml.write(to: kubeconfig, atomically: true, encoding: .utf8)

    let envFile = """
    export KUBECONFIG="\(kubeconfig.path)"
    export RUNE_K8S_AGENT=""
    export RUNE_FAKE_K8S_STATE="\(stateDir.path)"
    export RUNE_FAKE_K8S_BINARY="\(binaryPath)"
    export RUNE_FAKE_K8S_REST_SERVER="\(serverURL)"
    """
    try envFile.write(to: stateDir.appendingPathComponent("env.sh"), atomically: true, encoding: .utf8)
}

private func commandSummary() -> String {
    let fixtures = makeFixtures()
    return fixtures.map { cluster in
        let namespaceSummaries = cluster.namespaces.map { namespace in
            let podCount = pods(cluster: cluster, namespace: namespace).count
            return "  - \(namespace.name): \(podCount) pods, \(namespace.deploymentNames.count) deployments, \(namespace.cronJobNames.count) cronjobs"
        }.joined(separator: "\n")
        return "\(cluster.contextName) (default ns: \(cluster.defaultNamespace))\n\(namespaceSummaries)"
    }.joined(separator: "\n")
}

private func handleKubectl(arguments rawArgs: [String]) throws {
    let (stateDirOption, argsWithoutState) = parseOption(rawArgs, name: "--state-dir")
    _ = stateDirOption
    let (contextOverride, args) = globalContext(from: argsWithoutState)
    let defaultContext = makeFixtures().first?.contextName ?? "fake-orbit-mesh"
    let context = contextOverride ?? defaultContext

    guard let command = args.first else {
        throw CommandFailure(message: "fake kubectl requires a command")
    }

    switch command {
    case "config":
        guard args.count >= 2 else {
            throw CommandFailure(message: "Unsupported fake kubectl config invocation")
        }
        if args[1] == "get-contexts" {
            write(makeFixtures().map(\.contextName).joined(separator: "\n") + "\n")
            return
        }
        if args[1] == "view" {
            if let output = flagValue(args, short: "-o"), output == "jsonpath={..namespace}" {
                let cluster = try fixture(named: context)
                write(cluster.defaultNamespace)
                return
            }
            if let output = flagValue(args, short: "-o"), output == "json" {
                write(try renderConfig(context: context))
                return
            }
        }
        throw CommandFailure(message: "Unsupported fake kubectl config invocation: \(args.joined(separator: " "))")
    case "get":
        if args.count >= 3, args[1] == "--raw" {
            write(try renderRawProbe(cluster: try fixture(named: context), context: context, apiPath: args[2]))
            return
        }
        guard args.count >= 2 else {
            throw CommandFailure(message: "Unsupported fake kubectl get invocation")
        }
        let resource = args[1]
        let namespaceName = flagValue(args, short: "-n")
        let allNamespaces = hasFlag(args, short: "-A")
        let output = flagValue(args, short: "-o") ?? "json"
        let selector = flagValue(args, short: "-l")
        let cluster = try fixture(named: context)
        let ns = allNamespaces ? nil : try namespaceName.map { try namespaceFixture(cluster: cluster, namespace: $0) }

        if args.count >= 3, !args[2].hasPrefix("-") {
            let object = try objectByName(cluster: cluster, namespace: ns, resource: resource, name: args[2])
            switch output {
            case "json":
                write(try jsonString(object))
            case "yaml":
                write(yamlString(object) + "\n")
            default:
                throw CommandFailure(message: "Unsupported fake output format \(output) for named get")
            }
            return
        }

        var items = try resourceList(cluster: cluster, namespace: ns, resource: resource, allNamespaces: allNamespaces)
        if normalizedResourceName(resource) == "pods", let selector {
            let pair = selector.split(separator: "=", maxSplits: 1).map(String.init)
            if pair.count == 2 {
                items = items.filter { item in
                    let metadata = item["metadata"] as? [String: Any] ?? [:]
                    let labels = metadata["labels"] as? [String: String] ?? [:]
                    return labels[pair[0]] == pair[1]
                }
            }
        }

        switch output {
        case "json":
            write(try jsonString(["items": items]))
        default:
            if output.hasPrefix("custom-columns=") {
                let columns = String(output.dropFirst("custom-columns=".count))
                write(renderCustomColumns(resource: resource, items: items, columns: columns))
            } else {
                throw CommandFailure(message: "Unsupported fake get output format: \(output)")
            }
        }
    case "top":
        guard args.count >= 2 else {
            throw CommandFailure(message: "Unsupported fake kubectl top invocation")
        }
        let cluster = try fixture(named: context)
        let namespaceName = flagValue(args, short: "-n")
        let allNamespaces = hasFlag(args, short: "-A")
        switch normalizedResourceName(args[1]) {
        case "pods":
            let namespace = allNamespaces ? nil : try namespaceFixture(cluster: cluster, namespace: namespaceName ?? cluster.defaultNamespace)
            write(renderTopPods(cluster: cluster, namespace: namespace, allNamespaces: allNamespaces))
        case "nodes":
            write(renderTopNodes(cluster: cluster))
        default:
            throw CommandFailure(message: "Unsupported fake top resource: \(args[1])")
        }
    case "describe":
        guard args.count >= 3 else {
            throw CommandFailure(message: "Unsupported fake kubectl describe invocation")
        }
        let cluster = try fixture(named: context)
        let namespaceName = flagValue(args, short: "-n") ?? cluster.defaultNamespace
        let namespace = try namespaceFixture(cluster: cluster, namespace: namespaceName)
        let object = try objectByName(cluster: cluster, namespace: namespace, resource: args[1], name: args[2])
        write(describeObject(object))
    case "logs":
        guard args.count >= 2 else {
            throw CommandFailure(message: "Unsupported fake kubectl logs invocation")
        }
        let cluster = try fixture(named: context)
        let namespaceName = flagValue(args, short: "-n") ?? cluster.defaultNamespace
        let namespace = try namespaceFixture(cluster: cluster, namespace: namespaceName)
        let podName = args[1]
        guard let pod = pods(cluster: cluster, namespace: namespace).first(where: { $0.name == podName }) else {
            throw CommandFailure(message: "No fake pod named \(podName)")
        }
        write(podLogs(pod, previous: hasFlag(args, short: "--previous")))
    case "delete":
        guard args.count >= 3 else {
            throw CommandFailure(message: "Unsupported fake kubectl delete invocation")
        }
        write("\(args[1]) \"\(args[2])\" deleted\n")
    case "scale":
        guard args.count >= 3 else {
            throw CommandFailure(message: "Unsupported fake kubectl scale invocation")
        }
        write("\(args[1]) \"\(args[2])\" scaled\n")
    case "rollout":
        guard args.count >= 3 else {
            throw CommandFailure(message: "Unsupported fake kubectl rollout invocation")
        }
        switch args[1] {
        case "restart":
            write("\(args[2]) restarted\n")
        case "history":
            write("deployment.apps/\(args[2])\nREVISION  CHANGE-CAUSE\n1         synthetic fixture bootstrap\n2         synthetic fixture rollout\n")
        case "undo":
            write("\(args[2]) rolled back\n")
        default:
            throw CommandFailure(message: "Unsupported fake rollout operation: \(args[1])")
        }
    case "exec":
        guard let separator = args.firstIndex(of: "--"), separator > 1 else {
            throw CommandFailure(message: "Unsupported fake kubectl exec invocation")
        }
        let podName = args[1]
        let command = args[(separator + 1)...].joined(separator: " ")
        write("fake exec in \(podName): \(command)\n")
    case "apply":
        write("resources applied\n")
    case "patch":
        write("resource patched\n")
    case "create":
        write("job.batch/\(args.dropFirst(2).first ?? "synthetic") created\n")
    default:
        throw CommandFailure(message: "Unsupported fake kubectl command: \(command)")
    }
}

private func parseStateDir(_ args: [String], defaultPath: String) -> (URL, [String]) {
    let (value, remaining) = parseOption(args, name: "--state-dir")
    return (URL(fileURLWithPath: value ?? defaultPath), remaining)
}

do {
    let root = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
    let defaultStateDir = root.appendingPathComponent(".rune-fake-k8s").path
    var args = Array(CommandLine.arguments.dropFirst())

    guard let command = args.first else {
        write("""
        usage:
          RuneFakeK8s setup [--state-dir PATH] [--binary PATH]
          RuneFakeK8s setup-rest [--state-dir PATH] [--binary PATH] --server-url URL
          RuneFakeK8s serve [--host HOST] [--port PORT] [--context CONTEXT]
          RuneFakeK8s summary
        """)
        Foundation.exit(0)
    }

    args.removeFirst()

    switch command {
    case "setup":
        let (stateDir, remaining) = parseStateDir(args, defaultPath: defaultStateDir)
        let (binaryPath, _) = parseOption(remaining, name: "--binary")
        let resolvedBinary = binaryPath ?? CommandLine.arguments[0]
        try ensureSetup(stateDir: stateDir, binaryPath: resolvedBinary)
        write("fake kubeconfig: \(stateDir.appendingPathComponent("kubeconfig.yaml").path)\n")
        write(commandSummary() + "\n")
    case "setup-rest":
        let (stateDir, remaining) = parseStateDir(args, defaultPath: defaultStateDir)
        let (binaryPath, withoutBinary) = parseOption(remaining, name: "--binary")
        let (serverURL, _) = parseOption(withoutBinary, name: "--server-url")
        guard let serverURL, !serverURL.isEmpty else {
            throw CommandFailure(message: "setup-rest requires --server-url URL")
        }
        let resolvedBinary = binaryPath ?? CommandLine.arguments[0]
        try ensureRESTSetup(stateDir: stateDir, binaryPath: resolvedBinary, serverURL: serverURL)
        write("fake REST kubeconfig: \(stateDir.appendingPathComponent("kubeconfig.yaml").path)\n")
        write("server: \(serverURL)\n")
    case "summary":
        write(commandSummary() + "\n")
    case "serve":
        let (host, withoutHost) = parseOption(args, name: "--host")
        let (portString, withoutPort) = parseOption(withoutHost, name: "--port")
        let (contextName, _) = parseOption(withoutPort, name: "--context")
        let port = UInt16(portString ?? "0") ?? 0
        let stopSemaphore = DispatchSemaphore(value: 0)
        let server = try RuneFakeK8sRESTServer.startBlocking(
            host: host ?? "127.0.0.1",
            port: port,
            contextName: contextName ?? RuneFakeK8sFixture.defaultContextName
        )
        write("RuneFakeK8s REST listening on http://\(host ?? "127.0.0.1"):\(server.port)\n")
        withExtendedLifetime(server) {
            stopSemaphore.wait()
        }
    case "kubectl":
        try handleKubectl(arguments: args)
    default:
        throw CommandFailure(message: "Unknown RuneFakeK8s command: \(command)")
    }
} catch {
    fail(String(describing: error))
}
