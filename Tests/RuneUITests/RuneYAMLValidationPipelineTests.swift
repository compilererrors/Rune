import RuneCore
@testable import RuneUI
import XCTest

/// Validation re-runs after every edit path and always reflects the text on
/// screen: rapid edits never leave issues from an older text behind.
@MainActor
final class RuneYAMLValidationPipelineTests: XCTestCase {
    private let valid = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: sample\ndata:\n  key: value\n"
    private let tabbed = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n\tname: sample\n"
    private let missingKind = "apiVersion: v1\nmetadata:\n  name: sample\n"
    private let duplicate = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n  name: b\n"

    func testRapidDraftEditsEndOnTheLastTextsIssues() async throws {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, bookmarkManager: .inMemory())
        _ = viewModel
        state.setResourceYAML(valid)
        for text in [tabbed, valid, duplicate, valid, missingKind] {
            state.updateResourceYAMLDraft(text)
            await Task.yield()
        }
        try await settle(state, expecting: missingKind)
        XCTAssertEqual(state.resourceYAMLValidationIssues, YAMLValidator.validate(missingKind))
        XCTAssertEqual(state.resourceYAMLValidationIssues.map(\.message), ["Missing kind"])
    }

    func testEveryEditPathRevalidates() async throws {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, bookmarkManager: .inMemory())
        _ = viewModel

        // Document load or switch.
        state.setResourceYAML(tabbed)
        try await settle(state, expecting: tabbed)
        XCTAssertEqual(state.resourceYAMLValidationIssues.first?.line, 4)

        // Inline edit, Quick Edit and the editor sheet all write the draft.
        state.updateResourceYAMLDraft(valid)
        try await settle(state, expecting: valid)
        XCTAssertEqual(state.resourceYAMLValidationIssues, [])

        state.updateResourceYAMLDraft(duplicate)
        try await settle(state, expecting: duplicate)
        XCTAssertEqual(state.resourceYAMLValidationIssues.first?.source, .syntax)

        // Revert back to the loaded document.
        state.setResourceYAML(valid)
        try await settle(state, expecting: valid)
        XCTAssertEqual(state.resourceYAMLValidationIssues, [])
    }

    func testIssuesAreNeverPublishedForAStaleText() async throws {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, bookmarkManager: .inMemory())
        _ = viewModel
        state.setResourceYAML(valid)
        try await settle(state, expecting: valid)

        // Start validating an invalid text, then fix it before the debounce ends.
        state.updateResourceYAMLDraft(tabbed)
        try await Task.sleep(nanoseconds: 20_000_000)
        state.updateResourceYAMLDraft(valid)
        try await settle(state, expecting: valid)
        try await Task.sleep(nanoseconds: 300_000_000)
        XCTAssertEqual(state.resourceYAMLValidationIssues, [], "the tab error from the older text never lands")
    }

    private func settle(_ state: RuneAppState, expecting text: String) async throws {
        let deadline = Date().addingTimeInterval(3)
        while Date() < deadline {
            try await Task.sleep(nanoseconds: 30_000_000)
            if state.resourceYAML == text,
               !state.isValidatingResourceYAML,
               state.resourceYAMLValidationIssues == YAMLValidator.validate(text) {
                return
            }
        }
        XCTFail("Validation did not settle for the current text; issues: \(state.resourceYAMLValidationIssues.map(\.message))")
    }
}
