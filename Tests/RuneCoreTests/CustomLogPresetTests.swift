import XCTest
@testable import RuneCore

final class CustomLogPresetTests: XCTestCase {
    func testLargeCustomLinePresetKeepsExactTailCount() {
        let config = RuneCustomLogPresetConfig(
            mode: .lines,
            lines: 999_999,
            timeValue: 15,
            timeUnit: .minutes
        )

        XCTAssertEqual(config.filter, .tailLines(999_999))
    }

    func testLargeCustomLinePresetTitleKeepsExactTailCount() {
        let config = RuneCustomLogPresetConfig(
            mode: .lines,
            lines: 999_999,
            timeValue: 15,
            timeUnit: .minutes
        )

        XCTAssertEqual(config.title(slot: .one), "Custom 1 (999999 lines)")
    }

    func testCustomTimeUnitsMapToMatchingFilters() {
        XCTAssertEqual(RuneCustomLogPresetTimeUnit.minutes.makeFilter(amount: 7), .lastMinutes(7))
        XCTAssertEqual(RuneCustomLogPresetTimeUnit.hours.makeFilter(amount: 7), .lastHours(7))
        XCTAssertEqual(RuneCustomLogPresetTimeUnit.days.makeFilter(amount: 7), .lastDays(7))
    }

    func testBlankInvalidZeroAndOverflowValuesNormalizeToOne() {
        XCTAssertEqual(RuneCustomLogPresetConfig.normalizedPositiveInteger(""), 1)
        XCTAssertEqual(RuneCustomLogPresetConfig.normalizedPositiveInteger("not-a-number"), 1)
        XCTAssertEqual(RuneCustomLogPresetConfig.normalizedPositiveInteger("0"), 1)
        XCTAssertEqual(
            RuneCustomLogPresetConfig.normalizedPositiveInteger(String(repeating: "9", count: 100)),
            1
        )
    }

    func testSlotDefaultsMatchSettingsDefaults() {
        XCTAssertEqual(
            RuneCustomLogPresetConfig.defaultValue(for: .one),
            RuneCustomLogPresetConfig(
                mode: .lines,
                lines: 5_000,
                timeValue: 15,
                timeUnit: .minutes
            )
        )
        XCTAssertEqual(
            RuneCustomLogPresetConfig.defaultValue(for: .two),
            RuneCustomLogPresetConfig(
                mode: .time,
                lines: 99_999,
                timeValue: 6,
                timeUnit: .hours
            )
        )
    }

    func testMissingPersistedValuesUseSlotDefaults() throws {
        let suiteName = "CustomLogPresetTests.missing.\(UUID().uuidString)"
        let defaults = try XCTUnwrap(UserDefaults(suiteName: suiteName))
        defer { defaults.removePersistentDomain(forName: suiteName) }

        XCTAssertEqual(
            defaults.runeCustomLogPresetConfig(slot: .one),
            RuneCustomLogPresetConfig.defaultValue(for: .one)
        )
        XCTAssertEqual(
            defaults.runeCustomLogPresetConfig(slot: .two),
            RuneCustomLogPresetConfig.defaultValue(for: .two)
        )
    }

    func testStoredBlankAndOverflowValuesMatchSettingsSummaryNormalization() throws {
        let suiteName = "CustomLogPresetTests.invalid.\(UUID().uuidString)"
        let defaults = try XCTUnwrap(UserDefaults(suiteName: suiteName))
        defer { defaults.removePersistentDomain(forName: suiteName) }

        defaults.set("", forKey: RuneSettingsKeys.logsCustomPresetOneLines)
        defaults.set(
            String(repeating: "9", count: 100),
            forKey: RuneSettingsKeys.logsCustomPresetOneTimeValue
        )

        let config = defaults.runeCustomLogPresetConfig(slot: .one)
        XCTAssertEqual(config.lines, 1)
        XCTAssertEqual(config.timeValue, 1)
    }

    func testRelativeLogWindowsSaturateInsteadOfOverflowing() {
        XCTAssertEqual(LogTimeFilter.lastMinutes(0).kubernetesSinceSeconds, 60)
        XCTAssertEqual(LogTimeFilter.lastHours(-5).kubernetesSinceSeconds, 3_600)
        XCTAssertEqual(LogTimeFilter.lastDays(Int.max).kubernetesSinceSeconds, Int.max)
        XCTAssertEqual(LogTimeFilter.lastDays(Int.max).kubernetesSinceArgument, "\(Int.max)h")
    }

    @MainActor
    func testFullPodLogSnapshotRemainsActiveWhileWarmCacheIsBounded() {
        let state = RuneAppState()
        let firstLine = "synthetic-first-full-log-line"
        let lastLine = "synthetic-last-full-log-line"
        let logs = "\(firstLine)\n\(String(repeating: "x", count: 1_100_000))\n\(lastLine)"

        state.replacePodLogRead(
            logs,
            contextName: "synthetic-context",
            namespace: "synthetic-namespace",
            podName: "synthetic-pod"
        )

        XCTAssertTrue(state.podLogs.contains(firstLine))
        XCTAssertTrue(state.podLogs.contains(lastLine))
        XCTAssertFalse(state.podLogs.contains("[older session log cache truncated]"))

        let cached = state.cachedLogs(
            contextName: "synthetic-context",
            namespace: "synthetic-namespace",
            kind: .pod,
            resourceName: "synthetic-pod"
        )
        XCTAssertTrue(cached.contains("[older session log cache truncated]"))
        XCTAssertFalse(cached.contains(firstLine))
        XCTAssertTrue(cached.contains(lastLine))
    }

    @MainActor
    func testFullUnifiedLogSnapshotRemainsActiveWhileWarmCacheIsBounded() {
        let state = RuneAppState()
        let firstLine = "synthetic-first-unified-log-line"
        let lastLine = "synthetic-last-unified-log-line"
        let logs = "\(firstLine)\n\(String(repeating: "y", count: 1_100_000))\n\(lastLine)"

        state.replaceUnifiedServiceLogRead(
            logs,
            pods: ["synthetic-pod-a", "synthetic-pod-b"],
            contextName: "synthetic-context",
            namespace: "synthetic-namespace",
            kind: .deployment,
            resourceName: "synthetic-deployment"
        )

        XCTAssertTrue(state.unifiedServiceLogs.contains(firstLine))
        XCTAssertTrue(state.unifiedServiceLogs.contains(lastLine))
        XCTAssertFalse(state.unifiedServiceLogs.contains("[older session log cache truncated]"))

        let cached = state.cachedLogs(
            contextName: "synthetic-context",
            namespace: "synthetic-namespace",
            kind: .deployment,
            resourceName: "synthetic-deployment"
        )
        XCTAssertTrue(cached.contains("[older session log cache truncated]"))
        XCTAssertFalse(cached.contains(firstLine))
        XCTAssertTrue(cached.contains(lastLine))
    }
}
