import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class RuneIconButtonTests: XCTestCase {
    @MainActor
    func testIconButtonKeepsTwentyEightPointGeometryAcrossStates() {
        let regularHost = NSHostingView(rootView: RuneIconButton(
            "Refresh",
            systemImage: "arrow.clockwise",
            action: {}
        ))
        let selectedHost = NSHostingView(rootView: RuneIconButton(
            "Favorite",
            systemImage: "star.fill",
            isSelected: true,
            selectedTint: .yellow,
            action: {}
        ))
        let disabledHost = NSHostingView(rootView: RuneIconButton(
            "Next match",
            systemImage: "chevron.down",
            isDisabled: true,
            action: {}
        ))

        for host in [regularHost, selectedHost, disabledHost] {
            XCTAssertGreaterThanOrEqual(host.fittingSize.width, RuneUILayoutMetrics.iconButtonSize)
            XCTAssertGreaterThanOrEqual(host.fittingSize.height, RuneUILayoutMetrics.iconButtonSize)
            XCTAssertLessThanOrEqual(host.fittingSize.width, RuneUILayoutMetrics.iconButtonSize + 1)
            XCTAssertLessThanOrEqual(host.fittingSize.height, RuneUILayoutMetrics.iconButtonSize + 1)
        }
    }

    func testIconButtonOwnsHelpAccessibilityAndStateSemantics() throws {
        let source = try String(contentsOfFile: runeDesignComponentsPath, encoding: .utf8)

        XCTAssertTrue(source.contains("struct RuneIconButton: View"))
        XCTAssertTrue(source.contains(".font(.system(size: 11, weight: .semibold))"))
        XCTAssertTrue(source.contains("width: RuneUILayoutMetrics.iconButtonSize"))
        XCTAssertTrue(source.contains("height: RuneUILayoutMetrics.iconButtonSize"))
        XCTAssertTrue(source.contains(".contentShape(Rectangle())"))
        XCTAssertTrue(source.contains(".disabled(isDisabled)"))
        XCTAssertTrue(source.contains(".help(helpText)"))
        XCTAssertTrue(source.contains(".accessibilityLabel(accessibilityLabel)"))
        XCTAssertTrue(source.contains(".accessibilityValue(isSelected ? \"Selected\" : \"Not selected\")"))
        XCTAssertTrue(source.contains(".accessibilityAddTraits(isSelected ? .isSelected : [])"))
        XCTAssertTrue(source.contains("runeThemePalette?.mutedText"))
        XCTAssertTrue(source.contains("runeThemePalette?.secondaryText"))
    }

    func testIconButtonAdoptionStaysSelectiveAndComponentScoped() throws {
        let designSource = try String(contentsOfFile: runeDesignComponentsPath, encoding: .utf8)
        let findSource = try String(contentsOfFile: inspectorFindPath, encoding: .utf8)
        let contextRowSource = try String(contentsOfFile: contextSidebarRowPath, encoding: .utf8)
        let preferencesSource = try String(contentsOfFile: runePreferencesViewPath, encoding: .utf8)
        let logTabSource = try String(contentsOfFile: terminalLogTabBarPath, encoding: .utf8)
        let sessionTabSource = try String(contentsOfFile: terminalSessionTabBarPath, encoding: .utf8)
        let appKitTableSource = try String(contentsOfFile: appKitPodTableViewPath, encoding: .utf8)
        let rootSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(designSource.contains("RuneIconButton(\n                \"Dismiss notice\""))
        XCTAssertTrue(designSource.contains("RuneIconButton(accessibilityLabel, systemImage: \"xmark\", action: action)"))
        XCTAssertTrue(findSource.contains("RuneIconButton(\n                \"Previous match\""))
        XCTAssertTrue(findSource.contains("RuneIconButton(\n                \"Next match\""))
        XCTAssertTrue(findSource.contains("RuneIconButton(\"Close find\", systemImage: \"xmark\")"))
        XCTAssertTrue(contextRowSource.contains("isSelected: isFavorite"))
        XCTAssertTrue(contextRowSource.contains("selectedTint: .yellow"))
        XCTAssertTrue(preferencesSource.contains("RuneIconButton(\n            \"More info\""))
        XCTAssertTrue(logTabSource.contains("isSelected: tab.isFavorite"))
        XCTAssertTrue(logTabSource.contains("tab.isFavorite ? \"Remove Log Target Favorite\" : \"Favorite Log Target\""))
        XCTAssertTrue(logTabSource.contains("\"Close log tab for \\(tab.title)\""))
        XCTAssertTrue(logTabSource.contains(".accessibilityElement(children: .contain)"))
        XCTAssertTrue(logTabSource.contains("Button {\n                onSelectTab(tab.id)"))
        XCTAssertFalse(logTabSource.contains(".onTapGesture {\n            onSelectTab(tab.id)"))
        XCTAssertTrue(sessionTabSource.contains("RuneIconButton(\"Close terminal tab\", systemImage: \"xmark\")"))
        XCTAssertTrue(sessionTabSource.contains(".accessibilityElement(children: .contain)"))
        XCTAssertTrue(sessionTabSource.contains("Button {\n                select(session)"))
        XCTAssertFalse(sessionTabSource.contains(".onTapGesture {\n            select(session)"))
        XCTAssertTrue(appKitTableSource.contains("button.widthAnchor.constraint(equalToConstant: RuneUILayoutMetrics.iconButtonSize)"))
        XCTAssertTrue(appKitTableSource.contains("button.heightAnchor.constraint(equalToConstant: RuneUILayoutMetrics.iconButtonSize)"))
        XCTAssertTrue(appKitTableSource.contains("button.setAccessibilityValue(isFavorite ? \"Selected\" : \"Not selected\")"))
        XCTAssertTrue(appKitTableSource.contains("button.setAccessibilityHelp(button.toolTip)"))
        XCTAssertTrue(rootSource.contains("RuneIconButton(\n                \"Copy \\(label)\","))
        XCTAssertTrue(rootSource.contains("RuneIconButton(\n            \"Refresh inspector\","))
        XCTAssertFalse(rootSource.contains(".frame(width: 22, height: 22)"))
    }

    private var runeDesignComponentsPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Layout/RuneDesignComponents.swift").path
    }

    private var inspectorFindPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/InspectorFind.swift").path
    }

    private var contextSidebarRowPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/ContextSidebarRow.swift").path
    }

    private var runePreferencesViewPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/RunePreferencesView.swift").path
    }

    private var terminalLogTabBarPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalLogTabBar.swift").path
    }

    private var terminalSessionTabBarPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalSessionTabBar.swift").path
    }

    private var appKitPodTableViewPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/AppKitPodTableView.swift").path
    }

    private var runeRootViewPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift").path
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}
