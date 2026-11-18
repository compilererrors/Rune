import RuneCore
import XCTest

/// The shell commands people ran, kept for the Terminal's Commands tab:
/// newest first, without repeats in a row, within the limit set in
/// Settings, and none at all when the limit is 0.
final class TerminalCommandHistoryTests: XCTestCase {
    private let date = Date(timeIntervalSince1970: 1_800_000_000)

    private func record(_ commands: [String], limit: Int = 50) -> TerminalCommandHistory {
        var history = TerminalCommandHistory()
        for (offset, command) in commands.enumerated() {
            history.record(command, contextName: "sample-cluster", namespace: "team-alpha", podName: "api-0",
                           at: date.addingTimeInterval(TimeInterval(offset)), limit: limit)
        }
        return history
    }

    func testNewestCommandsComeFirst() {
        XCTAssertEqual(record(["pwd", "ls -la", "df -h"]).entries.map(\.command), ["df -h", "ls -la", "pwd"])
    }

    func testARepeatedCommandMovesToTheTopInsteadOfDuplicating() {
        XCTAssertEqual(record(["pwd", "ls", "pwd"]).entries.map(\.command), ["pwd", "ls"])
    }

    func testBlankCommandsAreNotKept() {
        XCTAssertEqual(record(["  ", "", "ls"]).entries.map(\.command), ["ls"])
    }

    func testTheLimitKeepsTheNewest() {
        let history = record((1...8).map { "echo \($0)" }, limit: 3)
        XCTAssertEqual(history.entries.map(\.command), ["echo 8", "echo 7", "echo 6"])
        var trimmed = history
        trimmed.apply(limit: 1)
        XCTAssertEqual(trimmed.entries.map(\.command), ["echo 8"])
    }

    func testALimitOfZeroKeepsNothing() {
        XCTAssertTrue(record(["ls", "pwd"], limit: 0).entries.isEmpty)
    }

    func testEntriesRememberWhereTheyRan() throws {
        let entry = try XCTUnwrap(record(["ls"]).entries.first)
        XCTAssertEqual(entry.contextName, "sample-cluster")
        XCTAssertEqual(entry.namespace, "team-alpha")
        XCTAssertEqual(entry.podName, "api-0")
        XCTAssertEqual(entry.date, date)
    }

    func testItRoundTripsThroughStorage() throws {
        let history = record(["pwd", "ls -la"])
        let decoded = try JSONDecoder().decode(TerminalCommandHistory.self, from: JSONEncoder().encode(history))
        XCTAssertEqual(decoded, history)
    }

    func testTheLimitSettingIsClamped() {
        XCTAssertEqual(RuneSettingsKeys.terminalCommandHistoryLimitDefault, 50)
        XCTAssertEqual(RuneSettingsKeys.clampedTerminalCommandHistoryLimit(-5), 0)
        XCTAssertEqual(RuneSettingsKeys.clampedTerminalCommandHistoryLimit(50_000), RuneSettingsKeys.terminalCommandHistoryLimitMaximum)
    }

    func testTurningHistoryOffMakesTheLimitZeroAndRemovesSavedHistory() throws {
        let suiteName = "TerminalCommandHistoryTests.\(UUID().uuidString)"
        let defaults = try XCTUnwrap(UserDefaults(suiteName: suiteName))
        defer { defaults.removePersistentDomain(forName: suiteName) }

        XCTAssertTrue(defaults.runeTerminalCommandHistoryEnabled, "on by default")
        XCTAssertEqual(defaults.runeEffectiveTerminalCommandHistoryLimit, RuneSettingsKeys.terminalCommandHistoryLimitDefault)
        defaults.set(10, forKey: RuneSettingsKeys.terminalCommandHistoryLimit)
        XCTAssertEqual(defaults.runeEffectiveTerminalCommandHistoryLimit, 10)

        defaults.set(Data("[]".utf8), forKey: RuneSettingsKeys.terminalCommandHistory)
        defaults.runeTerminalCommandHistoryEnabled = false
        XCTAssertEqual(defaults.runeEffectiveTerminalCommandHistoryLimit, 0)
        XCTAssertNil(defaults.object(forKey: RuneSettingsKeys.terminalCommandHistory))

        defaults.runeTerminalCommandHistoryEnabled = true
        XCTAssertEqual(defaults.runeEffectiveTerminalCommandHistoryLimit, 10, "the limit is kept while off")
    }
}
