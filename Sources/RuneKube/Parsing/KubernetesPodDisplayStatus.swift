import Foundation

enum KubernetesPodDisplayStatus {
    struct Terminated: Equatable {
        var reason: String?
        var exitCode: Int
        var signal: Int
    }

    struct ContainerState: Equatable {
        var name: String
        var ready: Bool
        var started: Bool?
        var waitingReason: String?
        var running: Bool
        var terminated: Terminated?
    }

    struct Condition: Equatable {
        var type: String
        var status: String
        var reason: String?
    }

    struct Input: Equatable {
        var phase: String
        var reason: String?
        var isDeleting: Bool
        var conditions: [Condition]
        var containerCount: Int
        var initContainers: [(name: String, restartable: Bool)]
        var initContainerStatuses: [ContainerState]
        var containerStatuses: [ContainerState]

        static func == (lhs: Input, rhs: Input) -> Bool {
            lhs.phase == rhs.phase && lhs.reason == rhs.reason && lhs.isDeleting == rhs.isDeleting
                && lhs.conditions == rhs.conditions && lhs.containerCount == rhs.containerCount
                && lhs.initContainers.map(\.name) == rhs.initContainers.map(\.name)
                && lhs.initContainers.map(\.restartable) == rhs.initContainers.map(\.restartable)
                && lhs.initContainerStatuses == rhs.initContainerStatuses
                && lhs.containerStatuses == rhs.containerStatuses
        }
    }

    struct Result: Equatable {
        var status: String
        var readyContainers: Int
        var totalContainers: Int

        var readyLine: String? {
            totalContainers > 0 ? "\(readyContainers)/\(totalContainers)" : nil
        }
    }

    static func compute(_ input: Input) -> Result {
        let restartable = Dictionary(
            input.initContainers.map { ($0.name, $0.restartable) },
            uniquingKeysWith: { first, _ in first }
        )
        let sidecarCount = input.initContainers.filter(\.restartable).count
        var reason = input.phase.isEmpty ? "Unknown" : input.phase
        if let podReason = nonEmpty(input.reason) {
            reason = podReason
        }
        if input.conditions.contains(where: { $0.type == "PodScheduled" && $0.reason == "SchedulingGated" }) {
            reason = "SchedulingGated"
        }

        var ready = 0
        var initializing = false
        for (index, container) in input.initContainerStatuses.enumerated() {
            if let terminated = container.terminated, terminated.exitCode == 0 {
                continue
            }
            if restartable[container.name] == true, container.started == true {
                if container.ready { ready += 1 }
                continue
            }
            if let terminated = container.terminated {
                if let terminatedReason = nonEmpty(terminated.reason) {
                    reason = "Init:" + terminatedReason
                } else if terminated.signal != 0 {
                    reason = "Init:Signal:\(terminated.signal)"
                } else {
                    reason = "Init:ExitCode:\(terminated.exitCode)"
                }
            } else if let waiting = nonEmpty(container.waitingReason), waiting != "PodInitializing" {
                reason = "Init:" + waiting
            } else {
                reason = "Init:\(index)/\(input.initContainers.count)"
            }
            initializing = true
            break
        }

        let initialized = input.conditions.contains { $0.type == "Initialized" && $0.status == "True" }
        if !initializing || initialized {
            var hasRunning = false
            for container in input.containerStatuses.reversed() {
                if let waiting = nonEmpty(container.waitingReason) {
                    reason = waiting
                } else if let terminated = container.terminated {
                    if let terminatedReason = nonEmpty(terminated.reason) {
                        reason = terminatedReason
                    } else if terminated.signal != 0 {
                        reason = "Signal:\(terminated.signal)"
                    } else {
                        reason = "ExitCode:\(terminated.exitCode)"
                    }
                } else if container.ready && container.running {
                    hasRunning = true
                    ready += 1
                }
            }
            if reason == "Completed" && hasRunning {
                let podReady = input.conditions.contains { $0.type == "Ready" && $0.status == "True" }
                reason = podReady ? "Running" : "NotReady"
            }
        }

        if input.isDeleting {
            if input.reason == "NodeLost" {
                reason = "Unknown"
            } else if input.phase != "Succeeded" && input.phase != "Failed" {
                reason = "Terminating"
            }
        }

        return Result(status: reason, readyContainers: ready, totalContainers: input.containerCount + sidecarCount)
    }

    static func input(metadata: [String: Any], spec: [String: Any]?, status: [String: Any]?) -> Input {
        let initContainers = (spec?["initContainers"] as? [[String: Any]] ?? []).compactMap { container -> (name: String, restartable: Bool)? in
            guard let name = container["name"] as? String else { return nil }
            return (name, (container["restartPolicy"] as? String) == "Always")
        }
        let conditions = (status?["conditions"] as? [[String: Any]] ?? []).compactMap { condition -> Condition? in
            guard let type = condition["type"] as? String else { return nil }
            return Condition(type: type, status: condition["status"] as? String ?? "", reason: condition["reason"] as? String)
        }
        return Input(
            phase: (status?["phase"] as? String ?? "").trimmingCharacters(in: .whitespacesAndNewlines),
            reason: status?["reason"] as? String,
            isDeleting: nonEmpty(metadata["deletionTimestamp"] as? String) != nil,
            conditions: conditions,
            containerCount: (spec?["containers"] as? [[String: Any]])?.count ?? 0,
            initContainers: initContainers,
            initContainerStatuses: containerStates(status?["initContainerStatuses"]),
            containerStatuses: containerStates(status?["containerStatuses"])
        )
    }

    private static func containerStates(_ raw: Any?) -> [ContainerState] {
        (raw as? [[String: Any]] ?? []).map { container in
            let state = container["state"] as? [String: Any]
            let waiting = state?["waiting"] as? [String: Any]
            let terminated = (state?["terminated"] as? [String: Any]).map { terminated in
                Terminated(
                    reason: terminated["reason"] as? String,
                    exitCode: intValue(terminated["exitCode"]),
                    signal: intValue(terminated["signal"])
                )
            }
            return ContainerState(
                name: container["name"] as? String ?? "",
                ready: container["ready"] as? Bool ?? false,
                started: container["started"] as? Bool,
                waitingReason: waiting?["reason"] as? String,
                running: state?["running"] != nil,
                terminated: terminated
            )
        }
    }

    private static func intValue(_ raw: Any?) -> Int {
        if let value = raw as? Int { return value }
        if let value = raw as? NSNumber { return value.intValue }
        return 0
    }

    private static func nonEmpty(_ value: String?) -> String? {
        guard let trimmed = value?.trimmingCharacters(in: .whitespacesAndNewlines), !trimmed.isEmpty else { return nil }
        return trimmed
    }
}
