import XCTest
@testable import RuneCore
@testable import RuneUI

final class TerminalSessionDetailPresentationTests: XCTestCase {
    func testIncludesContainerForContainerScopedSession() {
        let presentation = TerminalSessionDetailPresentation.make(session: PodTerminalSession(
            id: "shell-sidecar",
            contextName: "synthetic",
            namespace: "default",
            podName: "api-0",
            containerName: "sidecar",
            shell: "sh",
            status: .failed,
            lastExitCode: 137
        ))

        XCTAssertEqual(presentation.targetTitle, "default/api-0")
        XCTAssertEqual(presentation.containerTitle, "sidecar")
        XCTAssertEqual(presentation.shellTitle, "sh")
        XCTAssertEqual(presentation.statusTitle, "Failed")
        XCTAssertEqual(presentation.lastExitCodeTitle, "137")
    }

    func testOmitsContainerForDefaultSession() {
        let presentation = TerminalSessionDetailPresentation.make(session: PodTerminalSession(
            id: "shell-default",
            contextName: "synthetic",
            namespace: "default",
            podName: "api-0",
            shell: "sh",
            status: .connected
        ))

        XCTAssertNil(presentation.containerTitle)
        XCTAssertEqual(presentation.statusTitle, "Connected")
        XCTAssertNil(presentation.lastExitCodeTitle)
    }
}
