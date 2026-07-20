import Foundation
import XCTest
@testable import RuneCore

final class PodSummaryLogContainerTests: XCTestCase {
    func testLogContainerNamesIncludeMainInitAndEphemeralWithoutChangingExecContainers() {
        let pod = PodSummary(
            name: "synthetic-pod",
            namespace: "synthetic",
            status: "Running",
            containerNamesLine: "main, sidecar",
            initContainerNamesLine: "setup, main",
            ephemeralContainerNamesLine: "debugger, sidecar"
        )

        XCTAssertEqual(pod.containerNames, ["main", "sidecar"])
        XCTAssertEqual(pod.initContainerNames, ["setup", "main"])
        XCTAssertEqual(pod.ephemeralContainerNames, ["debugger", "sidecar"])
        XCTAssertEqual(pod.logContainerNames, ["main", "sidecar", "setup", "debugger"])
    }

    func testLegacyCodablePayloadWithoutNewContainerFieldsStillDecodes() throws {
        let legacy = Data(
            """
            {
              "name": "legacy-pod",
              "namespace": "synthetic",
              "status": "Running",
              "totalRestarts": 0,
              "ageDescription": "1m",
              "containerNamesLine": "main, sidecar",
              "labels": {}
            }
            """.utf8
        )

        let pod = try JSONDecoder().decode(PodSummary.self, from: legacy)

        XCTAssertEqual(pod.containerNames, ["main", "sidecar"])
        XCTAssertNil(pod.initContainerNamesLine)
        XCTAssertNil(pod.ephemeralContainerNamesLine)
        XCTAssertEqual(pod.logContainerNames, ["main", "sidecar"])
    }

    func testInspectorMergePreservesEveryContainerFamily() {
        let base = PodSummary(
            name: "synthetic-pod",
            namespace: "synthetic",
            status: "Running",
            containerNamesLine: "main",
            initContainerNamesLine: "old-setup",
            ephemeralContainerNamesLine: "old-debugger"
        )
        let detail = PodSummary(
            name: "synthetic-pod",
            namespace: "synthetic",
            status: "Running",
            containerNamesLine: "main, sidecar",
            initContainerNamesLine: "setup",
            ephemeralContainerNamesLine: "debugger"
        )

        let merged = base.mergingInspectorDetail(detail)

        XCTAssertEqual(merged.containerNames, ["main", "sidecar"])
        XCTAssertEqual(merged.initContainerNames, ["setup"])
        XCTAssertEqual(merged.ephemeralContainerNames, ["debugger"])
        XCTAssertEqual(merged.logContainerNames, ["main", "sidecar", "setup", "debugger"])
    }
}
