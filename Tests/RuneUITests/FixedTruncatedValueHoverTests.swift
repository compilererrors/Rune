import XCTest
@testable import RuneUI

final class FixedTruncatedValueHoverTests: XCTestCase {
    func testFixedWorkspaceSelectorsAndRelationshipRowsExposeFullValues() throws {
        let root = try source("Sources/RuneUI/Views/RuneRootView.swift")
        let terminal = try source("Sources/RuneUI/Views/TerminalShellPanelView.swift")
        let relationships = try source("Sources/RuneUI/Views/ResourceRelationshipViews.swift")
        let rollout = try source("Sources/RuneUI/Views/DeploymentRolloutHistoryView.swift")

        XCTAssertTrue(root.contains(".help(viewModel.state.selectedContext?.name ?? \"No Context\")"))
        XCTAssertTrue(root.contains(".help(namespaceMenuTitle)"))
        XCTAssertTrue(terminal.contains("? \"Default container\"\n                        : selectedTerminalContainerName)"))
        XCTAssertTrue(relationships.contains(".help(\"Open \\(title) — \\(subtitle)\")"))
        XCTAssertTrue(rollout.contains(".help(row.replicaSet)"))
        XCTAssertTrue(rollout.contains(".help(row.changeCause.isEmpty ? \"No change cause\" : row.changeCause)"))
        XCTAssertTrue(rollout.contains(".help(value)"))
    }

    func testFixedReviewAndCommandSurfacesExposeUnabridgedContent() throws {
        let review = try source("Sources/RuneUI/Views/KubeConfigImportReviewPanel.swift")
        let terminal = try source("Sources/RuneUI/Views/ResourceTerminalInspectorView.swift")
        let addCluster = try source("Sources/RuneUI/Views/AddClusterPopoverView.swift")
        let yaml = try source("Sources/RuneUI/Views/ResourceYAMLInspectorView.swift")
        let preferences = try source("Sources/RuneUI/Views/RunePreferencesView.swift")

        XCTAssertTrue(review.contains(".help(sourceName)"))
        XCTAssertGreaterThanOrEqual(
            review.components(separatedBy: ".help(context.name)").count - 1,
            3
        )
        XCTAssertGreaterThanOrEqual(
            review.components(separatedBy: ".help(contextDetailText(context))").count - 1,
            2
        )
        XCTAssertTrue(terminal.contains(".help(\"Insert into terminal prompt: \\(command)\")"))
        XCTAssertTrue(addCluster.contains(".help(\"\\(title) — \\(subtitle)\")"))
        XCTAssertTrue(addCluster.contains(".help(\"\\(provider.title) — \\(presentation.compactSubtitle)\")"))
        XCTAssertTrue(yaml.contains(".help(text)"))
        XCTAssertTrue(preferences.contains(".help(displayThemesDirectoryPath)"))
        XCTAssertTrue(preferences.contains(".help(DebugTraceWriter.logFileURL.path)"))
        XCTAssertTrue(preferences.contains(".help(\"\\(presentation.title) — \\(presentation.sourceSummary)\")"))
    }

    private func source(_ relativePath: String) throws -> String {
        try String(
            contentsOf: repoRoot.appendingPathComponent(relativePath),
            encoding: .utf8
        )
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}
