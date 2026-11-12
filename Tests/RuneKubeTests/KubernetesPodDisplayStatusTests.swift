import Foundation
import XCTest
@testable import RuneCore
@testable import RuneKube

final class KubernetesPodDisplayStatusTests: XCTestCase {
    private typealias Status = KubernetesPodDisplayStatus

    private func running(_ name: String, ready: Bool = true) -> Status.ContainerState {
        Status.ContainerState(name: name, ready: ready, started: true, waitingReason: nil, running: true, terminated: nil)
    }

    private func waiting(_ name: String, _ reason: String) -> Status.ContainerState {
        Status.ContainerState(name: name, ready: false, started: false, waitingReason: reason, running: false, terminated: nil)
    }

    private func terminated(_ name: String, reason: String?, exitCode: Int, signal: Int = 0) -> Status.ContainerState {
        Status.ContainerState(
            name: name, ready: false, started: false, waitingReason: nil, running: false,
            terminated: Status.Terminated(reason: reason, exitCode: exitCode, signal: signal)
        )
    }

    private func input(
        phase: String = "Running",
        reason: String? = nil,
        isDeleting: Bool = false,
        conditions: [Status.Condition] = [],
        containerCount: Int = 1,
        initContainers: [(name: String, restartable: Bool)] = [],
        initStatuses: [Status.ContainerState] = [],
        statuses: [Status.ContainerState]
    ) -> Status.Input {
        Status.Input(
            phase: phase, reason: reason, isDeleting: isDeleting, conditions: conditions,
            containerCount: containerCount, initContainers: initContainers,
            initContainerStatuses: initStatuses, containerStatuses: statuses
        )
    }

    func testHealthyPodShowsPhaseAndReadyCount() {
        let result = Status.compute(input(containerCount: 2, statuses: [running("app"), running("proxy")]))
        XCTAssertEqual(result.status, "Running")
        XCTAssertEqual(result.readyLine, "2/2")
    }

    func testWaitingContainerReasonReplacesRunningPhase() {
        let result = Status.compute(input(statuses: [waiting("app", "CrashLoopBackOff")]))
        XCTAssertEqual(result.status, "CrashLoopBackOff")
        XCTAssertEqual(result.readyLine, "0/1")
    }

    func testNativeSidecarCountsTowardReady() {
        let result = Status.compute(input(
            conditions: [Status.Condition(type: "Initialized", status: "True", reason: nil)],
            initContainers: [("mesh-proxy", true)],
            initStatuses: [running("mesh-proxy")],
            statuses: [running("app")]
        ))
        XCTAssertEqual(result.status, "Running")
        XCTAssertEqual(result.readyLine, "2/2")
    }

    func testUnstartedSidecarKeepsPodInitializing() {
        let result = Status.compute(input(
            phase: "Pending",
            initContainers: [("mesh-proxy", true)],
            initStatuses: [waiting("mesh-proxy", "PodInitializing")],
            statuses: [waiting("app", "PodInitializing")]
        ))
        XCTAssertEqual(result.status, "Init:0/1")
        XCTAssertEqual(result.readyLine, "0/2")
    }

    func testClassicInitContainersReportProgressAndFailures() {
        let progress = Status.compute(input(
            phase: "Pending",
            initContainers: [("migrate", false), ("seed", false)],
            initStatuses: [terminated("migrate", reason: "Completed", exitCode: 0), waiting("seed", "PodInitializing")],
            statuses: [waiting("app", "PodInitializing")]
        ))
        XCTAssertEqual(progress.status, "Init:1/2")
        XCTAssertEqual(progress.readyLine, "0/1")

        let crashLoop = Status.compute(input(
            phase: "Pending",
            initContainers: [("migrate", false)],
            initStatuses: [waiting("migrate", "CrashLoopBackOff")],
            statuses: [waiting("app", "PodInitializing")]
        ))
        XCTAssertEqual(crashLoop.status, "Init:CrashLoopBackOff")

        let exitCode = Status.compute(input(
            phase: "Pending",
            initContainers: [("migrate", false)],
            initStatuses: [terminated("migrate", reason: nil, exitCode: 3)],
            statuses: []
        ))
        XCTAssertEqual(exitCode.status, "Init:ExitCode:3")
    }

    func testTerminatedContainerReasonAndExitCode() {
        XCTAssertEqual(Status.compute(input(phase: "Succeeded", statuses: [terminated("job", reason: "Completed", exitCode: 0)])).status, "Completed")
        XCTAssertEqual(Status.compute(input(statuses: [terminated("app", reason: "OOMKilled", exitCode: 137)])).status, "OOMKilled")
        XCTAssertEqual(Status.compute(input(statuses: [terminated("app", reason: nil, exitCode: 0, signal: 9)])).status, "Signal:9")
    }

    func testCompletedContainerWithRunningPeerShowsReadiness() {
        let notReady = Status.compute(input(
            containerCount: 2,
            statuses: [running("app"), terminated("worker", reason: "Completed", exitCode: 0)]
        ))
        XCTAssertEqual(notReady.status, "NotReady")

        let ready = Status.compute(input(
            conditions: [Status.Condition(type: "Ready", status: "True", reason: nil)],
            containerCount: 2,
            statuses: [running("app"), terminated("worker", reason: "Completed", exitCode: 0)]
        ))
        XCTAssertEqual(ready.status, "Running")
    }

    func testPodReasonGatesAndDeletion() {
        XCTAssertEqual(Status.compute(input(phase: "Failed", reason: "Evicted", statuses: [])).status, "Evicted")
        XCTAssertEqual(Status.compute(input(
            phase: "Pending",
            conditions: [Status.Condition(type: "PodScheduled", status: "False", reason: "SchedulingGated")],
            statuses: []
        )).status, "SchedulingGated")
        XCTAssertEqual(Status.compute(input(isDeleting: true, statuses: [running("app")])).status, "Terminating")
        XCTAssertEqual(Status.compute(input(phase: "Succeeded", isDeleting: true, statuses: [terminated("job", reason: "Completed", exitCode: 0)])).status, "Completed")
        XCTAssertEqual(Status.compute(input(reason: "NodeLost", isDeleting: true, statuses: [running("app")])).status, "Unknown")
    }

    private let sidecarAndCrashLoopList = #"""
    {"apiVersion":"v1","kind":"PodList","items":[
      {"metadata":{"name":"mesh-app","namespace":"alpha-zone","creationTimestamp":"2026-01-01T00:00:00Z"},
       "spec":{"initContainers":[{"name":"mesh-proxy","image":"example/proxy:1","restartPolicy":"Always"}],
               "containers":[{"name":"app","image":"example/app:1"}]},
       "status":{"phase":"Running",
                 "conditions":[{"type":"Initialized","status":"True"},{"type":"Ready","status":"True"}],
                 "initContainerStatuses":[{"name":"mesh-proxy","ready":true,"started":true,"restartCount":0,"state":{"running":{"startedAt":"2026-01-01T00:00:01Z"}}}],
                 "containerStatuses":[{"name":"app","ready":true,"started":true,"restartCount":0,"state":{"running":{"startedAt":"2026-01-01T00:00:02Z"}}}]}},
      {"metadata":{"name":"crashing-app","namespace":"alpha-zone","creationTimestamp":"2026-01-01T00:00:00Z"},
       "spec":{"containers":[{"name":"app","image":"example/app:1"}]},
       "status":{"phase":"Running",
                 "conditions":[{"type":"Initialized","status":"True"},{"type":"Ready","status":"False"}],
                 "containerStatuses":[{"name":"app","ready":false,"started":false,"restartCount":7,"state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}}
    ]}
    """#

    func testListParserReportsSidecarReadinessAndWaitingReason() throws {
        let pods = try KubernetesOutputParser().parsePodsListJSON(namespace: "alpha-zone", from: sidecarAndCrashLoopList)
        let mesh = try XCTUnwrap(pods.first { $0.name == "mesh-app" })
        XCTAssertEqual(mesh.status, "Running")
        XCTAssertNil(mesh.statusReason)
        XCTAssertEqual(mesh.displayStatus, "Running")
        XCTAssertEqual(mesh.containersReady, "2/2")

        let crashing = try XCTUnwrap(pods.first { $0.name == "crashing-app" })
        XCTAssertEqual(crashing.status, "Running")
        XCTAssertEqual(crashing.statusReason, "CrashLoopBackOff")
        XCTAssertEqual(crashing.displayStatus, "CrashLoopBackOff")
        XCTAssertEqual(crashing.containersReady, "0/1")
    }

    func testSinglePodParserMatchesListParser() throws {
        let single = #"""
        {"metadata":{"name":"terminating-app","namespace":"alpha-zone","creationTimestamp":"2026-01-01T00:00:00Z","deletionTimestamp":"2026-01-02T00:00:00Z"},
         "spec":{"initContainers":[{"name":"mesh-proxy","restartPolicy":"Always"}],"containers":[{"name":"app"}]},
         "status":{"phase":"Running","conditions":[{"type":"Initialized","status":"True"}],
                   "initContainerStatuses":[{"name":"mesh-proxy","ready":true,"started":true,"state":{"running":{}}}],
                   "containerStatuses":[{"name":"app","ready":true,"started":true,"state":{"running":{}}}]}}
        """#
        let pod = try KubernetesOutputParser().parseSinglePodJSON(namespace: "alpha-zone", from: single)
        XCTAssertEqual(pod.status, "Running")
        XCTAssertEqual(pod.displayStatus, "Terminating")
        XCTAssertEqual(pod.containersReady, "2/2")
    }

    func testPodSummaryDecodesSnapshotsWithoutStatusReason() throws {
        let legacy = #"{"name":"legacy","namespace":"alpha-zone","status":"Running","totalRestarts":0,"ageDescription":"1d","labels":{}}"#
        let pod = try JSONDecoder().decode(PodSummary.self, from: Data(legacy.utf8))
        XCTAssertNil(pod.statusReason)
        XCTAssertEqual(pod.displayStatus, "Running")
    }

    func testOperatorVersionCandidatesFollowDiscovery() {
        let served = KubernetesClient.parseServedAPIGroups(#"""
        {"kind":"APIGroupList","groups":[
          {"name":"external-secrets.io","versions":[{"groupVersion":"external-secrets.io/v1","version":"v1"}],
           "preferredVersion":{"groupVersion":"external-secrets.io/v1","version":"v1"}},
          {"name":"argoproj.io","versions":[{"groupVersion":"argoproj.io/v1alpha1","version":"v1alpha1"}],
           "preferredVersion":{"groupVersion":"argoproj.io/v1alpha1","version":"v1alpha1"}}
        ]}
        """#)

        XCTAssertEqual(
            KubernetesClient.operatorResourceVersionCandidates(group: "external-secrets.io", knownVersions: ["v1", "v1beta1"], servedGroups: served),
            ["v1"]
        )
        XCTAssertEqual(
            KubernetesClient.operatorResourceVersionCandidates(group: "argoproj.io", knownVersions: ["v1alpha1"], servedGroups: served),
            ["v1alpha1"]
        )
        XCTAssertEqual(
            KubernetesClient.operatorResourceVersionCandidates(group: "cert-manager.io", knownVersions: ["v1"], servedGroups: served),
            [],
            "A group the cluster doesn't serve isn't requested."
        )
        XCTAssertEqual(
            KubernetesClient.operatorResourceVersionCandidates(group: "external-secrets.io", knownVersions: ["v1", "v1beta1"], servedGroups: nil),
            ["v1", "v1beta1"],
            "Without discovery, known versions are tried newest first."
        )
    }

    func testOperatorVersionCandidatesPreferServerPreferredVersion() {
        let served = KubernetesClient.parseServedAPIGroups(#"""
        {"groups":[{"name":"external-secrets.io",
          "versions":[{"version":"v1"},{"version":"v1beta1"}],
          "preferredVersion":{"version":"v1beta1"}}]}
        """#)
        XCTAssertEqual(
            KubernetesClient.operatorResourceVersionCandidates(group: "external-secrets.io", knownVersions: ["v1", "v1beta1"], servedGroups: served),
            ["v1beta1", "v1"]
        )
    }
}
