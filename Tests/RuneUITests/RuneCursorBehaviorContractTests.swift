import Foundation
import XCTest

final class RuneCursorBehaviorContractTests: XCTestCase {
    func testSharedCursorBehaviorMapsPointerAndTextInputIntents() throws {
        let source = try source(at: "Sources/RuneUI/Layout/RuneCursorBehavior.swift")
        let pointerAPI = try block(
            in: source,
            startingAt: "func runePointerCursor()",
            endingBefore: "func runeTextInputCursor()"
        )
        let textInputAPI = try block(
            in: source,
            startingAt: "func runeTextInputCursor()",
            endingBefore: "\n}"
        )

        XCTAssertTrue(pointerAPI.contains("intent: .pointer"))
        XCTAssertFalse(pointerAPI.contains("intent: .textInput"))
        XCTAssertTrue(textInputAPI.contains("intent: .textInput"))
        XCTAssertFalse(textInputAPI.contains("intent: .pointer"))
        XCTAssertTrue(source.contains("case .pointer:\n            return .arrow"))
        XCTAssertTrue(source.contains("case .textInput:\n            return .iBeam"))

        XCTAssertTrue(source.contains("override func updateTrackingAreas()"))
        XCTAssertTrue(source.contains("options: [.cursorUpdate, .activeInKeyWindow, .inVisibleRect]"))
        XCTAssertTrue(source.contains("override func cursorUpdate(with event: NSEvent)"))
        XCTAssertTrue(source.contains("intent.cursor.set()"))
        XCTAssertTrue(source.contains("private struct RuneCursorScopeModifier"))
        XCTAssertTrue(source.contains(".transformAnchorPreference("))
        XCTAssertTrue(source.contains(".mouseMoved"))
        XCTAssertTrue(source.contains(".mouseEnteredAndExited"))
        XCTAssertTrue(source.contains("override func mouseMoved(with event: NSEvent)"))
        XCTAssertTrue(source.contains("func cursorIntent(at point: NSPoint)"))
        XCTAssertFalse(source.contains("resetCursorRects()"))
        XCTAssertFalse(source.contains("addCursorRect("))
        XCTAssertTrue(
            source.contains("override func hitTest(_ point: NSPoint) -> NSView?"),
            "A cursor region must not consume clicks intended for the control above it."
        )
        XCTAssertFalse(source.contains(".onHover"))
        XCTAssertFalse(source.contains("cursor.push()"))
        XCTAssertFalse(source.contains("NSCursor.pop()"))
    }

    func testSharedFindChromeUsesPointerWhileInspectorInputsUseTextCursor() throws {
        let chromeSource = try source(at: "Sources/RuneUI/Layout/RuneFindBarControls.swift")
        let inspectorSource = try source(at: "Sources/RuneUI/Views/InspectorFind.swift")
        let findChrome = try block(
            in: chromeSource,
            startingAt: "struct RuneFindBarChrome",
            endingBefore: "struct RuneMatchCaseButton"
        )
        let mainSearchField = try block(
            in: inspectorSource,
            startingAt: "private var searchField: some View",
            endingBefore: "private var navigationControls"
        )
        let jumpDialog = try block(
            in: inspectorSource,
            startingAt: "private var jumpToMatchPopover: some View",
            endingBefore: "private func prepareJumpPopover"
        )

        XCTAssertTrue(findChrome.contains(".runePointerCursor()"))
        assertTextFieldUsesTextCursor(mainSearchField, named: "Inspector search field")
        assertTextFieldUsesTextCursor(jumpDialog, named: "Inspector match-number field")
        XCTAssertTrue(
            jumpDialog.contains(".runePointerCursor()"),
            "Non-input space in the inspector match dialog must use the pointer cursor."
        )
        XCTAssertTrue(
            jumpDialog.contains(".runeCursorScope(default: .pointer)"),
            "The match dialog must route its pointer and text-input regions through one cursor scope."
        )
    }

    func testTerminalFindRemovesPrivateArrowOverrideAndRestoresTextCursor() throws {
        let source = try source(at: "Sources/RuneUI/Views/TerminalTranscriptSurface.swift")
        let searchBar = try block(
            in: source,
            startingAt: "struct TerminalTranscriptSearchBar",
            endingBefore: "struct TerminalTranscriptRenderModel"
        )
        let mainSearchField = try block(
            in: searchBar,
            startingAt: "private var searchField: some View",
            endingBefore: "private var navigationControls"
        )

        assertTextFieldUsesTextCursor(mainSearchField, named: "Terminal search field")
        XCTAssertFalse(mainSearchField.contains(".runePointerCursor()"))
        XCTAssertFalse(source.contains("TerminalSearchCursorModifier"))
        XCTAssertFalse(source.contains("terminalSearchCursor("))
    }

    func testLogFindChromeAndBothInputsUseTheirCorrectCursors() throws {
        let source = try source(at: "Sources/RuneUI/Views/ResourceLogsInspectorView.swift")
        let searchBar = try block(
            in: source,
            startingAt: "struct ResourceLogsSearchBar",
            endingBefore: "struct ResourceLogSearchResult"
        )
        let body = try block(
            in: searchBar,
            startingAt: "var body: some View",
            endingBefore: "private var regularSearchAccessories"
        )
        let jumpDialog = try block(
            in: searchBar,
            startingAt: "private func jumpToMatchPopover",
            endingBefore: "private func commitJump"
        )

        XCTAssertTrue(
            body.contains(".runePointerCursor()"),
            "Non-input space in the log find chrome must use the pointer cursor."
        )
        assertTextFieldUsesTextCursor(body, named: "Log search field")
        assertTextFieldUsesTextCursor(jumpDialog, named: "Log match-number field")
        XCTAssertTrue(
            jumpDialog.contains(".runePointerCursor()"),
            "Non-input space in the log match dialog must use the pointer cursor."
        )
    }

    func testCommandPaletteAndPrefixHelpKeepPointerAroundSearchInput() throws {
        let source = try source(at: "Sources/RuneUI/Views/CommandPaletteView.swift")
        let palette = try block(
            in: source,
            startingAt: "struct CommandPaletteView",
            endingBefore: "private func prefixShortcut"
        )
        let prefixHelp = try block(
            in: source,
            startingAt: "private var fullPrefixHelp: some View",
            endingBefore: "private func applyPrefix"
        )

        assertPointerSurface(palette, named: "Command Palette")
        assertTextFieldUsesTextCursor(palette, named: "Command Palette search field")
        assertPointerSurface(prefixHelp, named: "Command Palette prefix-help popover")
    }

    func testYAMLSheetUsesPointerAroundTextEditorCursorRegion() throws {
        let yamlSource = try source(at: "Sources/RuneUI/Views/ResourceYAMLInspectorView.swift")
        let findSource = try source(at: "Sources/RuneUI/Views/InspectorFind.swift")
        let appKitSource = try source(at: "Sources/RuneUI/Views/AppKitManifestTextView.swift")
        let editorSurface = try block(
            in: yamlSource,
            startingAt: "struct ResourceYAMLEditorSurface",
            endingBefore: "struct ResourceYAMLInspectorPane"
        )
        let editorSheet = try block(
            in: yamlSource,
            startingAt: "struct ResourceYAMLEditorSheetView",
            endingBefore: "struct YAMLValidationSummaryView"
        )

        assertPointerSurface(editorSheet, named: "YAML editor sheet")
        XCTAssertTrue(editorSheet.contains("ResourceYAMLEditorSurface("))
        XCTAssertTrue(editorSurface.contains("TextEditor("))
        XCTAssertTrue(editorSurface.contains("AppKitManifestTextView("))
        XCTAssertFalse(
            editorSurface.contains(".runeTextInputCursor()"),
            "The entire YAML surface must not claim an I-beam over its gutter, border, and scrollbars."
        )

        let findOverlay = try block(
            in: findSource,
            startingAt: "if isFindPresented {",
            endingBefore: "} else {"
        )
        XCTAssertTrue(findOverlay.contains(".padding(RuneUILayoutMetrics.inspectorOverlayInset)"))
        XCTAssertTrue(
            findOverlay.contains(".runePointerCursor()"),
            "The expanded find overlay, including its outer padding, must cover the editor I-beam."
        )
        XCTAssertTrue(
            findOverlay.contains(".runeCursorScope(default: .pointer)"),
            "The expanded find overlay must use one frontmost cursor scope over the editable text view."
        )

        let gutter = try block(
            in: appKitSource,
            startingAt: "final class YAMLLineNumberGutterOverlayView",
            endingBefore: "private final class PlainManifestTextView"
        )
        XCTAssertTrue(
            gutter.contains("options: [.cursorUpdate, .activeInKeyWindow, .inVisibleRect]"),
            "The gutter must use the same modern cursor-update mechanism as overlapping editor chrome."
        )
        XCTAssertTrue(
            gutter.contains("NSCursor.arrow.set()"),
            "The non-selectable line-number gutter must keep the pointer cursor."
        )
    }

    func testKubeconfigReviewUsesPointerAroundMetadataTextFields() throws {
        let sheetSource = try source(at: "Sources/RuneUI/Views/KubeConfigImportReviewSheet.swift")
        let panelSource = try source(at: "Sources/RuneUI/Views/KubeConfigImportReviewPanel.swift")
        let metadataField = try block(
            in: panelSource,
            startingAt: "struct KubeConfigImportMetadataDraftField",
            endingBefore: "private enum KubeConfigImportReviewLayoutCoordinateSpace"
        )

        assertPointerSurface(sheetSource, named: "Kubeconfig review sheet")
        assertTextFieldUsesTextCursor(metadataField, named: "Kubeconfig metadata field")
    }

    func testManualNamespaceAndProviderSheetsSeparatePointerFromInputs() throws {
        let rootSource = try source(at: "Sources/RuneUI/Views/RuneRootView.swift")
        let inputSource = try source(at: "Sources/RuneUI/Views/AddClusterProviderCredentialField.swift")
        let namespaceSheet = try block(
            in: rootSource,
            startingAt: "private var manualNamespaceSheet: some View",
            endingBefore: "private func startLiveDebugScenarioIfNeeded"
        )
        let providerSheet = try block(
            in: rootSource,
            startingAt: "private func addClusterProviderSheet",
            endingBefore: "private func addClusterCloudImportOutputView"
        )
        let providerTextInput = try block(
            in: inputSource,
            startingAt: "struct AddClusterProviderCredentialTextInput",
            endingBefore: "\n}"
        )

        assertPointerSurface(namespaceSheet, named: "Manual namespace sheet")
        assertTextFieldUsesTextCursor(namespaceSheet, named: "Manual namespace field")
        assertPointerSurface(providerSheet, named: "Add-provider sheet")
        XCTAssertTrue(providerTextInput.contains("TextField("))
        XCTAssertTrue(providerTextInput.contains("SecureField("))
        XCTAssertGreaterThanOrEqual(
            occurrences(of: ".runeTextInputCursor()", in: providerTextInput),
            2,
            "Both ordinary and secure provider inputs must use an I-beam."
        )
    }

    func testAddClusterPopoverUsesPointerAroundAllManualInputs() throws {
        let source = try source(at: "Sources/RuneUI/Views/AddClusterPopoverView.swift")
        let popoverRoot = try block(
            in: source,
            startingAt: "struct AddClusterPopoverView",
            endingBefore: "private var header"
        )
        let manualFields = try block(
            in: source,
            startingAt: "private var manualTokenSection",
            endingBefore: "private var gridColumns"
        )

        assertPointerSurface(popoverRoot, named: "Add Cluster popover")
        XCTAssertGreaterThanOrEqual(occurrences(of: "TextField(", in: manualFields), 3)
        XCTAssertTrue(manualFields.contains("SecureField("))
        XCTAssertGreaterThanOrEqual(
            occurrences(of: ".runeTextInputCursor()", in: manualFields),
            4,
            "Every manual Add Cluster input must override the popover pointer with an I-beam."
        )
    }

    func testFavoriteAndComparisonPopoversUsePointerCursor() throws {
        let favoriteSource = try source(at: "Sources/RuneUI/Views/FavoritePodPicker.swift")
        let rootSource = try source(at: "Sources/RuneUI/Views/RuneRootView.swift")
        let favoritePopover = try block(
            in: favoriteSource,
            startingAt: "private var podPopover: some View",
            endingBefore: "private func podRow"
        )
        let comparisonPopover = try block(
            in: rootSource,
            startingAt: "private var genericResourceComparisonPopover: some View",
            endingBefore: "private var overviewCardModules"
        )

        assertPointerSurface(favoritePopover, named: "Favorite pod popover")
        assertPointerSurface(comparisonPopover, named: "Resource comparison popover")
    }

    private func assertPointerSurface(
        _ source: String,
        named name: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertTrue(
            source.contains(".runePointerCursor()"),
            "\(name) must use the pointer over its non-input chrome.",
            file: file,
            line: line
        )
    }

    private func assertTextFieldUsesTextCursor(
        _ source: String,
        named name: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertTrue(source.contains("TextField("), "\(name) is missing.", file: file, line: line)
        XCTAssertTrue(
            source.contains(".runeTextInputCursor()"),
            "\(name) must explicitly override surrounding pointer chrome with an I-beam cursor.",
            file: file,
            line: line
        )
    }

    private func occurrences(of needle: String, in source: String) -> Int {
        guard !needle.isEmpty else { return 0 }
        var count = 0
        var remaining = source[...]
        while let range = remaining.range(of: needle) {
            count += 1
            remaining = remaining[range.upperBound...]
        }
        return count
    }

    private func source(at relativePath: String) throws -> String {
        try String(
            contentsOf: repositoryRoot.appendingPathComponent(relativePath),
            encoding: .utf8
        )
    }

    private func block(
        in source: String,
        startingAt startMarker: String,
        endingBefore endMarker: String
    ) throws -> String {
        let start = try XCTUnwrap(
            source.range(of: startMarker),
            "Could not locate \(startMarker)."
        )
        let end = try XCTUnwrap(
            source.range(
                of: endMarker,
                range: start.upperBound..<source.endIndex
            ),
            "Could not locate \(endMarker) after \(startMarker)."
        )
        return String(source[start.lowerBound..<end.lowerBound])
    }

    private var repositoryRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}
