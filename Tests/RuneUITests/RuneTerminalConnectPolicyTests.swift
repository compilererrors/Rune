@testable import RuneUI
import XCTest

/// Pod Shell connects as soon as a pod is chosen, except in read-only mode
/// and on production contexts, where Connect stays a deliberate click on
/// the terminal itself.
final class RuneTerminalConnectPolicyTests: XCTestCase {
    func testAutoConnectOnlyWhereOpeningAShellIsSafeByDefault() {
        XCTAssertTrue(TerminalShellConnectPolicy.autoConnects(isReadOnly: false, isProduction: false, canApplyMutations: true))
        XCTAssertFalse(TerminalShellConnectPolicy.autoConnects(isReadOnly: true, isProduction: false, canApplyMutations: true), "read-only")
        XCTAssertFalse(TerminalShellConnectPolicy.autoConnects(isReadOnly: false, isProduction: true, canApplyMutations: true), "production")
        XCTAssertFalse(TerminalShellConnectPolicy.autoConnects(isReadOnly: false, isProduction: false, canApplyMutations: false), "no write access")
        XCTAssertFalse(TerminalShellConnectPolicy.autoConnects(isReadOnly: true, isProduction: true, canApplyMutations: false))
    }

    func testPickingAPodStartsItsShellOnlyWhenAutoConnectApplies() throws {
        let shell = try source("Sources/RuneUI/Views/TerminalShellPanelView.swift")
        let change = try XCTUnwrap(shell.slice(from: "private func handleShellPodSelectionChange", to: "private func autoConnectComposedTabIfNeeded"))
        XCTAssertTrue(change.contains("} else if autoConnects, let pod = availablePods.first(where: { $0.id == podID }) {\n            // Choosing a pod is the intent to open its shell.\n            onStartSession(pod, containerName)"))
        XCTAssertTrue(change.contains("onComposeNewSession()"), "otherwise a draft tab with Connect on the terminal")
        let composed = try XCTUnwrap(shell.slice(from: "private func autoConnectComposedTabIfNeeded", to: "private func performPrimaryAction"))
        XCTAssertTrue(composed.contains("guard autoConnects, isComposingNewSession, session == nil"))

        let root = try RuneRootViewSource.text()
        XCTAssertTrue(root.contains("isReadOnly: viewModel.state.isReadOnlyMode"))
        XCTAssertTrue(root.contains("isProduction: viewModel.isProductionContext"))
    }

    func testConnectLivesOnTheTerminalAndDisconnectInMenus() throws {
        let shell = try source("Sources/RuneUI/Views/TerminalShellPanelView.swift")
        XCTAssertTrue(shell.contains("showsPrimaryAction: false"), "no Connect button detached in the bar")
        XCTAssertTrue(shell.contains("TerminalConnectPrompt("), "Connect is part of the terminal")
        XCTAssertFalse(shell.contains(".overlay(alignment: hasTranscript ? .bottom : .center)"), "never on the transcript, never moving")
        XCTAssertTrue(shell.contains("failureReason: session?.lastDiagnostic?.summary"))
        XCTAssertTrue(shell.contains("Button(\"Disconnect\")"), "Disconnect in the … menu")
        let tabs = try source("Sources/RuneUI/Views/TerminalSessionTabBar.swift")
        XCTAssertTrue(tabs.contains("Button(\"Disconnect\") { onDisconnectSession(session.id) }"), "and in the tab's context menu")
        XCTAssertTrue(tabs.contains("statusDotColor: TerminalStatusStyling.color(session.status"), "each tab shows its state")
    }

    func testPromptTitlesNameTheState() {
        XCTAssertEqual(TerminalConnectPrompt.State.notConnected.title, "Not connected")
        XCTAssertEqual(TerminalConnectPrompt.State.connecting.title, "Connecting…")
        XCTAssertEqual(TerminalConnectPrompt.State.disconnected.title, "Disconnected")
        XCTAssertEqual(TerminalConnectPrompt.State.failed.title, "Connection failed")
    }

    private func source(_ path: String) throws -> String {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
        return try String(contentsOf: root.appendingPathComponent(path), encoding: .utf8)
    }
}

private extension String {
    func slice(from start: String, to end: String) -> String? {
        guard let lower = range(of: start)?.upperBound,
              let upper = range(of: end, range: lower..<endIndex)?.lowerBound else { return nil }
        return String(self[lower..<upper])
    }
}
