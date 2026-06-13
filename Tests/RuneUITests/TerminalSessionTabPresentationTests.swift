import XCTest
@testable import RuneCore
@testable import RuneUI

final class TerminalSessionTabPresentationTests: XCTestCase {
    func testIncludesContainerInTabPresentationWhenSessionTargetsContainer() {
        let presentation = TerminalSessionTabPresentation.make(
            session: PodTerminalSession(
                id: "shell-app",
                contextName: "synthetic",
                namespace: "default",
                podName: "api-0",
                containerName: "sidecar",
                shell: "sh",
                status: .connected
            ),
            number: 2
        )

        XCTAssertEqual(presentation.primaryTitle, "2 api-0")
        XCTAssertEqual(presentation.secondaryTitle, "sidecar")
        XCTAssertTrue(presentation.accessibilityLabel.contains("container sidecar"))
        XCTAssertTrue(presentation.helpText.contains("Container: sidecar"))
    }

    func testOmitsContainerFromTabPresentationForDefaultContainer() {
        let presentation = TerminalSessionTabPresentation.make(
            session: PodTerminalSession(
                id: "shell-default",
                contextName: "synthetic",
                namespace: "default",
                podName: "api-0",
                shell: "sh",
                status: .connected
            ),
            number: 1
        )

        XCTAssertEqual(presentation.primaryTitle, "1 api-0")
        XCTAssertNil(presentation.secondaryTitle)
        XCTAssertFalse(presentation.accessibilityLabel.contains("container"))
        XCTAssertFalse(presentation.helpText.contains("Container:"))
    }
}
