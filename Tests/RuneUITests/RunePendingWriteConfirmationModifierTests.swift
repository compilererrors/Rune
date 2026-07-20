import Foundation
import XCTest

final class RunePendingWriteConfirmationModifierTests: XCTestCase {
    func testRootUsesOneSharedPendingWritePresentationForMainAndEditorSurfaces() throws {
        let rootSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let modifierSource = try String(contentsOfFile: modifierPath, encoding: .utf8)

        XCTAssertEqual(
            rootSource.components(separatedBy: ".runePendingWriteConfirmation(").count - 1,
            2,
            "The main window and YAML sheet should share the same pending-write presentation."
        )
        XCTAssertFalse(rootSource.contains(".confirmationDialog("))
        XCTAssertTrue(rootSource.contains("showsCopyCommandAction: !viewModel.pendingWriteActionKubectlCommand.isEmpty"))
        XCTAssertTrue(rootSource.contains("onConfirm: confirmPendingWriteActionFromDialog"))
        XCTAssertTrue(rootSource.contains("onCancel: cancelPendingWriteActionFromDialog"))
        XCTAssertTrue(rootSource.contains("onCopyCommand: viewModel.copyPendingWriteActionKubectlCommand"))

        XCTAssertTrue(modifierSource.contains("if isDestructive"))
        XCTAssertTrue(modifierSource.contains("Button(confirmLabel, role: .destructive, action: onConfirm)"))
        XCTAssertTrue(modifierSource.contains("Button(confirmLabel, action: onConfirm)"))
        XCTAssertTrue(modifierSource.contains("Button(\"Cancel\", role: .cancel, action: onCancel)"))
        XCTAssertTrue(modifierSource.contains("if showsCopyCommandAction"))
        XCTAssertTrue(modifierSource.contains("Button(\"Copy kubectl command\", action: onCopyCommand)"))
    }

    private var runeRootViewPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift").path
    }

    private var modifierPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/RunePendingWriteConfirmationModifier.swift").path
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}
