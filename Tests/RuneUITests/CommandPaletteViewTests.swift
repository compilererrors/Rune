import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

@MainActor
final class CommandPaletteViewTests: XCTestCase {
    func testCommonPrefixShortcutsStaySmallAndProduceFocusedQueries() {
        let shortcuts = CommandPalettePresentation.commonPrefixShortcuts

        XCTAssertEqual(shortcuts.count, 4)
        XCTAssertEqual(shortcuts.map(\.queryPrefix), [":po", ":deploy", ":svc", ":ns"])
        XCTAssertEqual(
            shortcuts.map { CommandPalettePresentation.prefillQuery(for: $0.queryPrefix) },
            [":po ", ":deploy ", ":svc ", ":ns "]
        )
        XCTAssertEqual(Set(shortcuts.map(\.id)).count, shortcuts.count)
    }

    func testCheatSheetTitlesResolveToTheFirstActionablePrefix() {
        XCTAssertEqual(
            CommandPalettePresentation.prefillQuery(fromCheatSheetTitle: ":po <name>"),
            ":po "
        )
        XCTAssertEqual(
            CommandPalettePresentation.prefillQuery(fromCheatSheetTitle: ":svc / :service <name>"),
            ":svc "
        )
        XCTAssertEqual(
            CommandPalettePresentation.prefillQuery(fromCheatSheetTitle: "  :ctx <context>  "),
            ":ctx "
        )
        XCTAssertNil(CommandPalettePresentation.prefillQuery(fromCheatSheetTitle: ":"))
        XCTAssertNil(CommandPalettePresentation.prefillQuery(fromCheatSheetTitle: "Pods"))
    }

    func testPaletteGeometryStaysBoundedAtDefaultAndEnlargedTextSizes() {
        let viewModel = RuneAppViewModel(state: RuneAppState())
        let defaultHost = NSHostingView(rootView: AnyView(
            CommandPaletteView(viewModel: viewModel)
        ))
        let enlargedHost = NSHostingView(rootView: AnyView(
            CommandPaletteView(viewModel: viewModel)
                .dynamicTypeSize(.accessibility3)
        ))

        for host in [defaultHost, enlargedHost] {
            let size = host.fittingSize
            XCTAssertGreaterThanOrEqual(size.width, RuneUILayoutMetrics.commandPaletteMinWidth)
            XCTAssertLessThanOrEqual(size.width, RuneUILayoutMetrics.commandPaletteMaxWidth)
            XCTAssertGreaterThanOrEqual(size.height, RuneUILayoutMetrics.commandPaletteMinHeight)
            XCTAssertLessThanOrEqual(size.height, RuneUILayoutMetrics.commandPaletteMaxHeight)
        }
    }

    func testPaletteSourceUsesOneInstructionActionablePrefixesAndFullHelp() throws {
        let source = try String(contentsOfFile: commandPaletteViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("Search by name, or type : to browse command prefixes."))
        XCTAssertTrue(source.contains("TextField(\"Search commands and resources\""))
        XCTAssertFalse(source.contains("Search or use a prefix:"))
        XCTAssertFalse(source.contains("Search or type e.g."))
        XCTAssertFalse(source.contains("private func paletteHint"))
        XCTAssertTrue(source.contains("ForEach(CommandPalettePresentation.commonPrefixShortcuts)"))
        XCTAssertTrue(source.contains("applyPrefix(shortcut.queryPrefix)"))
        XCTAssertTrue(source.contains("Label(\"All Prefixes\", systemImage: \"questionmark.circle\")"))
        XCTAssertTrue(source.contains("let prefixItems = viewModel.commandPaletteItems(query: \":\")"))
        XCTAssertTrue(source.contains("ForEach(prefixItems)"))
        XCTAssertTrue(source.contains("CommandPalettePresentation.prefillQuery("))
        XCTAssertTrue(source.contains("fromCheatSheetTitle: item.title"))
        XCTAssertTrue(source.contains("title: \"No Commands Found\""))
        XCTAssertTrue(source.contains("private func executePrimaryAction(items: [CommandPaletteItem])"))
        XCTAssertTrue(source.contains("private func handleLocalKeyEvent(_ event: NSEvent, items: [CommandPaletteItem])"))
        XCTAssertTrue(source.contains("guard !isPrefixHelpPresented else { return false }"))
        XCTAssertTrue(source.contains("if !isPrefixHelpPresented {\n            VStack(spacing: 0)"))
        XCTAssertTrue(source.contains(".focused($prefixHelpFocusedItemID, equals: item.id)"))
    }

    private var commandPaletteViewPath: String {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/CommandPaletteView.swift")
            .path
    }
}
