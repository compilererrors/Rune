import XCTest
@testable import RuneKube

final class ProcessCommandRunnerTests: XCTestCase {
    func testProcessCommandRunnerDrainsLargeStdoutWhileProcessRuns() async throws {
        let runner = ProcessCommandRunner()

        let result = try await runner.run(
            executable: "/bin/sh",
            arguments: ["-c", "/usr/bin/yes rune-log-line | /usr/bin/head -n 50000"],
            timeout: 5
        )

        XCTAssertEqual(result.exitCode, 0)
        XCTAssertGreaterThan(result.stdout.utf8.count, 500_000)
        XCTAssertTrue(result.stdout.hasPrefix("rune-log-line\n"))
    }
}
