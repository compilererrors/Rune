import XCTest
@testable import RuneCore

final class RuneCoreTests: XCTestCase {
    func testRuneKeyboardShortcutParsesAndMatchesShiftBinding() {
        let shortcut = RuneKeyboardShortcut(storageValue: "shift-f")

        XCTAssertEqual(shortcut?.key, "f")
        XCTAssertEqual(shortcut?.displayValue, "Shift-F")
        XCTAssertTrue(shortcut?.matches(baseKey: "f", requiresShift: true) ?? false)
        XCTAssertFalse(shortcut?.matches(baseKey: "f", requiresShift: false) ?? true)
    }

    func testRuneKeyboardShortcutRejectsUnsupportedValues() {
        XCTAssertNil(RuneKeyboardShortcut(storageValue: "shift-/"))
        XCTAssertNil(RuneKeyboardShortcut(storageValue: "describe"))
        XCTAssertNil(RuneKeyboardShortcut(key: "-", requiresShift: false))
    }

    func testUserDefaultsFallsBackToDefaultRuneKeyBindingShortcut() {
        let suiteName = "RuneCoreTests-\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        let action = RuneKeyBindingAction.describe

        defer { defaults.removePersistentDomain(forName: suiteName) }

        XCTAssertEqual(defaults.runeKeyBindingShortcut(for: action), action.defaultShortcut)

        let custom = RuneKeyboardShortcut(key: "x", requiresShift: true)!
        defaults.setRuneKeyBindingShortcut(custom, for: action)

        XCTAssertEqual(defaults.runeKeyBindingShortcut(for: action), custom)
    }

    func testLogTimeFilterUsesSinceTimeOnlyForAbsoluteDate() {
        XCTAssertFalse(LogTimeFilter.lastMinutes(15).usesSinceTime)
        XCTAssertFalse(LogTimeFilter.lastHours(1).usesSinceTime)
        XCTAssertTrue(LogTimeFilter.since(Date(timeIntervalSince1970: 0)).usesSinceTime)
    }

    func testKubeConfigSourceUsesPathAsIdentifier() {
        let source = KubeConfigSource(url: URL(fileURLWithPath: "/tmp/kubeconfig"))

        XCTAssertEqual(source.id, "/tmp/kubeconfig")
        XCTAssertEqual(source.displayName, "kubeconfig")
    }

    func testKubernetesAgeDescribe() {
        let ref = Date(timeIntervalSince1970: 1_700_000_000)
        let age = KubernetesAgeFormatting.describe(creationISO8601: "2023-11-14T12:00:00Z", reference: ref)
        XCTAssertNotEqual(age, "—")
        XCTAssertFalse(age.isEmpty)
    }
}
