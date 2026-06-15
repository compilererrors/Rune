import XCTest
@testable import RuneUI

final class DeploymentRolloutHistoryPresentationTests: XCTestCase {
    func testRowsParseKubectlRolloutHistoryIntoStableColumns() {
        let rows = DeploymentRolloutHistoryPresentation.rows(from: """
        REVISION  REPLICASET                      CHANGE-CAUSE
        82        web-7956c7c8db                  <none>
        83        web-5fb7796f65                  kubectl apply
        84        web-55f9584664                  image updated by release job
        """)

        XCTAssertEqual(rows.count, 3)
        XCTAssertEqual(rows[0].revision, "82")
        XCTAssertEqual(rows[0].replicaSet, "web-7956c7c8db")
        XCTAssertEqual(rows[0].changeCause, "<none>")
        XCTAssertEqual(rows[2].revision, "84")
        XCTAssertEqual(rows[2].replicaSet, "web-55f9584664")
        XCTAssertEqual(rows[2].changeCause, "image updated by release job")
    }

    func testRowsIgnoreBlankAndMalformedLines() {
        let rows = DeploymentRolloutHistoryPresentation.rows(from: """

        REVISION  REPLICASET  CHANGE-CAUSE
        invalid
        1 api-abc
        """)

        XCTAssertEqual(rows, [
            DeploymentRolloutHistoryRow(revision: "1", replicaSet: "api-abc", changeCause: "")
        ])
    }

    func testRowsSplitAttachedNoneChangeCause() {
        let rows = DeploymentRolloutHistoryPresentation.rows(from: """
        REVISION  REPLICASET                  CHANGE-CAUSE
        82        web-7956c7c8db<none>
        """)

        XCTAssertEqual(rows, [
            DeploymentRolloutHistoryRow(revision: "82", replicaSet: "web-7956c7c8db", changeCause: "<none>")
        ])
    }
}
