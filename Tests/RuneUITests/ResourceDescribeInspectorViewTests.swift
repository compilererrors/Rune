import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

@MainActor
final class ResourceDescribeInspectorViewTests: XCTestCase {
    func testDescribeSurfaceUsesSharedInspectorReadOnlyTextSurface() throws {
        let source = try String(contentsOfFile: describeTextViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("InspectorTextSurface(minHeight: minHeight)"))
        XCTAssertTrue(source.contains("InspectorReadOnlyTextView("))
        XCTAssertTrue(source.contains("contentStyle: .describe"))
        XCTAssertTrue(source.contains("searchQuery: searchQuery"))
        XCTAssertFalse(source.contains("struct DescribeReadOnlyTextView"))
    }

    func testDescribeOutputHighlightsKubectlDescribeKeys() async throws {
        let describeText = """
        Name: api-0
        Namespace: default
        Containers:
          api:
            Image: example/app:1
        """
        let (host, window) = makeDescribeHost(describeText: describeText)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        guard let scrollView = findTextScrollView(in: host.view),
              let textView = scrollView.documentView as? NSTextView
        else {
            return XCTFail("Expected describe NSTextView-backed scroll view")
        }

        let source = textView.string as NSString
        let keyRange = source.range(of: "Name")
        let valueRange = source.range(of: "api-0")
        let sectionRange = source.range(of: "Containers")

        let keyColor = textView.textStorage?.attribute(.foregroundColor, at: keyRange.location, effectiveRange: nil) as? NSColor
        let valueColor = textView.textStorage?.attribute(.foregroundColor, at: valueRange.location, effectiveRange: nil) as? NSColor
        let sectionColor = textView.textStorage?.attribute(.foregroundColor, at: sectionRange.location, effectiveRange: nil) as? NSColor

        XCTAssertNotNil(keyColor)
        XCTAssertNotEqual(keyColor, valueColor, "Describe keys should be visually distinct from values, matching the k9s-style key/value scan pattern.")
        XCTAssertNotEqual(sectionColor, valueColor, "Describe section headers should stand out from plain values.")
    }

    func testManifestTextViewAppliesFindHighlightsAndActiveMatch() async throws {
        let text = Binding.constant("metadata:\n  name: api\n  labels:\n    app: api\n")
        let host = NSHostingController(
            rootView: AppKitManifestTextView(
                text: text,
                isEditable: false,
                contentStyle: .yaml,
                showsLineNumbers: true,
                searchQuery: "api",
                searchMatchCase: false,
                selectedSearchMatchIndex: 1
            )
            .frame(width: 460, height: 260)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 460, height: 260),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        let textView = try XCTUnwrap(findTextScrollView(in: host.view)?.documentView as? NSTextView)
        let source = textView.string as NSString
        let firstRange = source.range(of: "api")
        let secondRange = source.range(of: "api", options: [], range: NSRange(
            location: NSMaxRange(firstRange),
            length: source.length - NSMaxRange(firstRange)
        ))

        let firstBackground = textView.layoutManager?.temporaryAttribute(
            .backgroundColor,
            atCharacterIndex: firstRange.location,
            effectiveRange: nil
        ) as? NSColor
        let secondBackground = textView.layoutManager?.temporaryAttribute(
            .backgroundColor,
            atCharacterIndex: secondRange.location,
            effectiveRange: nil
        ) as? NSColor

        XCTAssertNotNil(firstBackground)
        XCTAssertNotNil(secondBackground)
        XCTAssertNotEqual(firstBackground, secondBackground, "The active find match should use a stronger highlight than passive matches.")
    }

    func testEditingAfterFindNavigationKeepsCaretAndHorizontalViewport() async throws {
        var source = "needle: " + String(repeating: "long-value-", count: 80)
        let binding = Binding(
            get: { source },
            set: { source = $0 }
        )
        let initialMatch = (source as NSString).range(of: "needle")
        let navigationRevision = 7
        let host = NSHostingController(
            rootView: AppKitManifestTextView(
                text: binding,
                isEditable: true,
                contentStyle: .yaml,
                searchQuery: "needle",
                searchMatchCase: false,
                selectedSearchMatchIndex: 0,
                searchMatchRanges: [initialMatch],
                searchNavigationRevision: navigationRevision
            )
            .frame(width: 340, height: 180)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 340, height: 180),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        let scrollView = try XCTUnwrap(findTextScrollView(in: host.view))
        let textView = try XCTUnwrap(scrollView.documentView as? NSTextView)
        let insertionLocation = (source as NSString).length
        let insertionRange = NSRange(location: insertionLocation, length: 0)
        textView.setSelectedRange(insertionRange)
        window.makeFirstResponder(textView)
        textView.scrollRangeToVisible(insertionRange)
        scrollView.reflectScrolledClipView(scrollView.contentView)
        try await settle(window: window)

        XCTAssertGreaterThan(
            scrollView.contentView.bounds.origin.x,
            0,
            "The long-line caret should require a horizontal viewport offset before editing."
        )

        textView.insertText("Z", replacementRange: insertionRange)
        let expectedCaret = NSRange(location: insertionLocation + 1, length: 0)
        let updatedMatch = (source as NSString).range(of: "needle")
        host.rootView = AppKitManifestTextView(
            text: binding,
            isEditable: true,
            contentStyle: .yaml,
            searchQuery: "needle",
            searchMatchCase: false,
            selectedSearchMatchIndex: 0,
            searchMatchRanges: [updatedMatch],
            searchNavigationRevision: navigationRevision
        )
        .frame(width: 340, height: 180)

        try await settle(window: window)

        XCTAssertEqual(
            textView.selectedRange(),
            expectedCaret,
            "Refreshing highlights after a keystroke must not restore the old find selection."
        )
        XCTAssertGreaterThan(
            scrollView.contentView.bounds.origin.x,
            0,
            "Refreshing highlights after a keystroke must not jump a long-line editor back to the leading edge."
        )

        host.rootView = AppKitManifestTextView(
            text: binding,
            isEditable: true,
            contentStyle: .yaml,
            searchQuery: "needle",
            searchMatchCase: false,
            selectedSearchMatchIndex: 0,
            searchMatchRanges: [updatedMatch],
            searchNavigationRevision: navigationRevision + 1
        )
        .frame(width: 340, height: 180)

        try await settle(window: window)

        XCTAssertEqual(
            textView.selectedRange(),
            updatedMatch,
            "An explicit find navigation revision must still reselect and focus the requested match."
        )
        XCTAssertEqual(
            scrollView.contentView.bounds.origin.x,
            0,
            accuracy: 1,
            "Explicitly navigating back to a leading-edge match should center that match."
        )
    }

    func testAsyncFindRefreshAfterEditingDoesNotCreateNavigationIntent() async throws {
        let model = EditableInspectorFindModel(
            text: "needle: " + String(repeating: "long-value-", count: 80)
        )
        let host = NSHostingController(
            rootView: EditableInspectorFindHarness(model: model)
                .frame(width: 340, height: 180)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 340, height: 180),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        let scrollView = try XCTUnwrap(findTextScrollView(in: host.view))
        let textView = try XCTUnwrap(scrollView.documentView as? NSTextView)
        let insertionLocation = (model.text as NSString).length
        let insertionRange = NSRange(location: insertionLocation, length: 0)
        textView.setSelectedRange(insertionRange)
        window.makeFirstResponder(textView)
        textView.scrollRangeToVisible(insertionRange)
        scrollView.reflectScrolledClipView(scrollView.contentView)
        try await settle(window: window)

        textView.insertText("Z", replacementRange: insertionRange)
        let expectedCaret = NSRange(location: insertionLocation + 1, length: 0)
        try await settle(window: window)
        try await settle(window: window)

        XCTAssertEqual(
            textView.selectedRange(),
            expectedCaret,
            "Publishing fresh asynchronous match ranges after an edit must only refresh highlights."
        )
        XCTAssertGreaterThan(
            scrollView.contentView.bounds.origin.x,
            0,
            "An asynchronous match refresh must preserve horizontal caret following."
        )
    }

    func testEditingNoMatchIntoMatchDoesNotStealCaretOrViewport() async throws {
        let model = EditableInspectorFindModel(
            text: "prefix: " + String(repeating: "long-value-", count: 80),
            query: "appears-later"
        )
        let host = NSHostingController(
            rootView: EditableInspectorFindHarness(model: model)
                .frame(width: 340, height: 180)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 340, height: 180),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        let scrollView = try XCTUnwrap(findTextScrollView(in: host.view))
        let textView = try XCTUnwrap(scrollView.documentView as? NSTextView)
        let insertionLocation = (model.text as NSString).length
        let insertionRange = NSRange(location: insertionLocation, length: 0)
        textView.setSelectedRange(insertionRange)
        window.makeFirstResponder(textView)
        textView.scrollRangeToVisible(insertionRange)
        scrollView.reflectScrolledClipView(scrollView.contentView)

        let insertedText = " appears-later"
        textView.insertText(insertedText, replacementRange: insertionRange)
        let expectedCaret = NSRange(
            location: insertionLocation + (insertedText as NSString).length,
            length: 0
        )
        try await settle(window: window)

        XCTAssertEqual(
            textView.selectedRange(),
            expectedCaret,
            "A text refresh that creates the first match is not an explicit find-navigation request."
        )
        XCTAssertGreaterThan(
            scrollView.contentView.bounds.origin.x,
            0,
            "Creating a match while editing a long line must preserve the caret's horizontal viewport."
        )
    }

    func testFindHighlightsStayDistinctAcrossBuiltInThemes() async throws {
        let source = "first api\nsecond api\n"

        for theme in RuneAppearanceTheme.allCases {
            let host = NSHostingController(
                rootView: AppKitManifestTextView(
                    text: .constant(source),
                    isEditable: false,
                    contentStyle: .plainText,
                    searchQuery: "api",
                    searchMatchCase: false,
                    selectedSearchMatchIndex: 1
                )
                .frame(width: 420, height: 180)
                .runeAppearanceTheme(theme.resolvedTheme)
            )
            let window = NSWindow(
                contentRect: NSRect(x: 0, y: 0, width: 420, height: 180),
                styleMask: [.titled, .closable],
                backing: .buffered,
                defer: false
            )
            window.contentViewController = host
            window.makeKeyAndOrderFront(nil)
            defer { window.orderOut(nil) }

            try await settle(window: window)
            let textView = try XCTUnwrap(findTextScrollView(in: host.view)?.documentView as? NSTextView)
            let nsSource = source as NSString
            let passiveRange = nsSource.range(of: "api")
            let activeRange = nsSource.range(
                of: "api",
                options: [],
                range: NSRange(
                    location: NSMaxRange(passiveRange),
                    length: nsSource.length - NSMaxRange(passiveRange)
                )
            )
            let passiveBackground = textView.layoutManager?.temporaryAttribute(
                .backgroundColor,
                atCharacterIndex: passiveRange.location,
                effectiveRange: nil
            ) as? NSColor
            let activeBackground = textView.layoutManager?.temporaryAttribute(
                .backgroundColor,
                atCharacterIndex: activeRange.location,
                effectiveRange: nil
            ) as? NSColor
            let passiveUnderline = textView.layoutManager?.temporaryAttribute(
                .underlineStyle,
                atCharacterIndex: passiveRange.location,
                effectiveRange: nil
            ) as? Int
            let activeUnderline = textView.layoutManager?.temporaryAttribute(
                .underlineStyle,
                atCharacterIndex: activeRange.location,
                effectiveRange: nil
            ) as? Int

            XCTAssertNotNil(passiveBackground, "\(theme.title) needs a visible passive find fill.")
            XCTAssertNotNil(activeBackground, "\(theme.title) needs a visible active find fill.")
            XCTAssertNotEqual(
                passiveBackground,
                activeBackground,
                "\(theme.title) must visually separate the active match from other matches."
            )
            XCTAssertEqual(passiveUnderline, NSUnderlineStyle.single.rawValue)
            XCTAssertEqual(activeUnderline, NSUnderlineStyle.thick.rawValue)
        }
    }

    func testDescribePaneDoesNotPlaceFooterBelowScrollableTextSurface() throws {
        let source = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)
        let sharedLayoutSource = try String(contentsOfFile: resourceManifestInspectorLayoutPath, encoding: .utf8)

        XCTAssertTrue(source.contains("ResourceManifestInspectorLayout"))
        XCTAssertTrue(sharedLayoutSource.contains(".layoutPriority(1)"))
        XCTAssertFalse(
            source.contains("The pane above is describe output from the cluster"),
            "Describe panes should behave like YAML panes: fixed controls above, one bounded scroll surface below. A footer after the scroll surface can push/clamp content outside the inspector."
        )
    }

    func testYAMLAndDescribeShareManifestInspectorLayout() throws {
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)
        let describeSource = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)
        let textSurfaceSource = try String(contentsOfFile: inspectorTextViewsPath, encoding: .utf8)
        let findSource = try String(contentsOfFile: inspectorFindPath, encoding: .utf8)
        let appKitManifestSource = try String(contentsOfFile: appKitManifestTextViewPath, encoding: .utf8)

        XCTAssertTrue(yamlSource.contains("ResourceManifestInspectorLayout"))
        XCTAssertTrue(describeSource.contains("ResourceManifestInspectorLayout"))
        XCTAssertTrue(textSurfaceSource.contains("GeometryReader"))
        XCTAssertTrue(textSurfaceSource.contains("height: max(1, proxy.size.height)"))
        XCTAssertTrue(yamlSource.contains("FindableInspectorSurface("))
        XCTAssertTrue(yamlSource.contains("placeholder: t(.findInYAML)"))
        XCTAssertTrue(describeSource.contains("FindableInspectorSurface("))
        XCTAssertTrue(describeSource.contains("placeholder: t(.findInDescribe)"))
        XCTAssertTrue(findSource.contains("struct InspectorFindBar"))
        XCTAssertTrue(findSource.contains(".keyboardShortcut(\"f\", modifiers: [.command])"))
        XCTAssertTrue(findSource.contains("RuneMatchCaseButton(isSelected: $matchCase)"))
        XCTAssertTrue(findSource.contains("RuneIconButton(\"Close find\", systemImage: \"xmark\")"))
        XCTAssertTrue(findSource.contains("RuneIconButton(\n                \"Previous match\""))
        XCTAssertTrue(findSource.contains("RuneIconButton(\n                \"Next match\""))
        XCTAssertTrue(findSource.contains("@State private var isJumpPopoverPresented = false"))
        XCTAssertTrue(findSource.contains("prepareJumpPopover()"))
        XCTAssertTrue(findSource.contains("private var jumpToMatchPopover: some View"))
        XCTAssertTrue(findSource.contains("private func commitJump()"))
        XCTAssertTrue(findSource.contains("Button(\"Go\", action: commitJump)"))
        XCTAssertTrue(appKitManifestSource.contains("func applySearchHighlights"))
        XCTAssertTrue(appKitManifestSource.contains("precomputedRanges"))
        XCTAssertTrue(appKitManifestSource.contains("centerSearchRange("))
        XCTAssertTrue(appKitManifestSource.contains("addTemporaryAttributes"))
    }

    func testUnsavedEditsIndicatorUsesReservedSlotToAvoidLayoutJumps() throws {
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)
        let describeSource = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(yamlSource.contains("ManifestUnsavedEditsSlot(isVisible: hasUnsavedEdits)"))
        XCTAssertTrue(describeSource.contains("if hasUnsavedEdits {\n                ManifestUnsavedEditsChip()"))
        XCTAssertTrue(yamlSource.contains("struct ManifestUnsavedEditsSlot"))
        XCTAssertTrue(yamlSource.contains(".opacity(isVisible ? 1 : 0)"))
        XCTAssertTrue(yamlSource.contains(".accessibilityHidden(!isVisible)"))
        XCTAssertTrue(yamlSource.contains(".allowsHitTesting(isVisible)"))
    }

    func testYAMLQuickEditResetsForDocumentChangeButNotBaselineRefresh() throws {
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)
        let rootSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let yamlPaneSource = try XCTUnwrap(yamlSource.slice(
            from: "struct ResourceYAMLInspectorPane",
            to: "struct ManifestUnsavedEditsChip"
        ))

        XCTAssertTrue(yamlPaneSource.contains("let documentIdentity: ResourceDetailScope?"))
        XCTAssertTrue(yamlPaneSource.contains(".onChange(of: documentIdentity)"))
        XCTAssertFalse(yamlPaneSource.contains(".onChange(of: baseline)"))
        XCTAssertTrue(rootSource.contains("documentIdentity: viewModel.state.resourceDetailScope"))
    }

    func testYAMLAndDescribeUseGroupedManifestToolbarAndCompactStatus() throws {
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)
        let describeSource = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)
        let presentationSource = try String(contentsOfFile: resourceManifestPresentationPath, encoding: .utf8)

        let yamlPaneSource = try XCTUnwrap(yamlSource.slice(
            from: "struct ResourceYAMLInspectorPane",
            to: "struct ManifestUnsavedEditsChip"
        ))
        let describePaneSource = describeSource

        XCTAssertTrue(yamlPaneSource.contains("ManifestActionToolbar("))
        XCTAssertTrue(presentationSource.contains("ManifestToolbarScrollRow"))
        XCTAssertTrue(presentationSource.contains("ManifestToolbarGroup(role: .action)"))
        XCTAssertTrue(presentationSource.contains("RuneUILayoutMetrics.inspectorToolbarGroupSpacing"))
        XCTAssertTrue(presentationSource.contains("RuneUILayoutMetrics.inspectorToolbarControlSpacing"))
        XCTAssertTrue(presentationSource.contains("RuneUILayoutMetrics.inspectorToolbarActionGroupHeight"))
        XCTAssertTrue(presentationSource.contains("RuneUILayoutMetrics.inspectorToolbarSourceGroupHeight"))
        XCTAssertTrue(yamlPaneSource.contains("statusText: statusText"))
        XCTAssertTrue(yamlPaneSource.contains("ManifestInlineNote("))
        XCTAssertTrue(yamlPaneSource.contains("if isInlineEditing"))
        XCTAssertTrue(yamlPaneSource.contains("} else if hasUnsavedEdits {"))
        XCTAssertFalse(yamlPaneSource.contains("VStack(alignment: .leading, spacing: 6)"))

        XCTAssertTrue(describePaneSource.contains("ManifestActionToolbar("))
        XCTAssertTrue(describePaneSource.contains("statusText: statusText"))
        XCTAssertTrue(describePaneSource.contains("Label(\"Edit YAML…\", systemImage: \"square.and.pencil\")"))
        XCTAssertFalse(describePaneSource.contains(".confirmationDialog("))
        XCTAssertTrue(describePaneSource.contains("if let staleContentState = documentState.staleContentState"))
        XCTAssertTrue(describePaneSource.contains("RuneContentStateView(staleContentState, variant: .inline)"))

        XCTAssertFalse(yamlPaneSource.contains("Text(statusText)"))
        XCTAssertFalse(describePaneSource.contains("Text(statusText)"))
    }

    func testDescribeOpensClearlyLabeledYAMLEditorWithoutRedundantConfirmation() throws {
        let describeSource = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(describeSource.contains("Label(\"Edit YAML…\", systemImage: \"square.and.pencil\")"))
        XCTAssertTrue(describeSource.contains(".accessibilityIdentifier(\"resource-describe-edit-yaml\")"))
        XCTAssertTrue(describeSource.contains("onOpenYAMLEditor()"))
        XCTAssertTrue(describeSource.contains("Changes stay local until you choose Apply."))
        XCTAssertFalse(describeSource.contains("isDescribeReadOnlyDialogPresented"))
        XCTAssertFalse(describeSource.contains(".confirmationDialog("))
    }

    func testYAMLPaneExposesManagedFieldsToggleWithoutHidingDuringEdit() throws {
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(yamlSource.contains("ManifestManagedFieldsToggle("))
        XCTAssertTrue(yamlSource.contains("Label(t(.hideManaged), systemImage: \"eye.slash\")"))
        XCTAssertTrue(yamlSource.contains("KubernetesManagedFieldsDisplayFilter.removingManagedFields"))
        XCTAssertTrue(yamlSource.contains("isDisabled: isInlineEditing"))
        XCTAssertTrue(yamlSource.contains("@AppStorage(RuneSettingsKeys.hideManagedFieldsByDefault)"))
        XCTAssertTrue(yamlSource.contains("let canHideManagedFields = filteredYAML.removedBlockCount > 0 && !isInlineEditing"))
        XCTAssertFalse(yamlSource.contains("hidesManagedFields = false"))
    }

    func testDescribePaneCanHideManagedFieldsWithoutChangingSourceDescribeText() throws {
        let describeSource = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(describeSource.contains("DescribeManagedFieldsDisplayFilter.removingManagedFields"))
        XCTAssertTrue(describeSource.contains("ManifestManagedFieldsToggle("))
        XCTAssertTrue(describeSource.contains("isDisabled: managedFieldsFilter.removedBlockCount == 0"))
        XCTAssertTrue(describeSource.contains("@AppStorage(RuneSettingsKeys.hideManagedFieldsByDefault)"))
        XCTAssertTrue(describeSource.contains("@AppStorage(RuneSettingsKeys.simpleMode)"))
        XCTAssertTrue(describeSource.contains("let effectiveHidesManagedFields = simpleMode || hidesManagedFields"))
        XCTAssertTrue(describeSource.contains("let presentedDescribeText = effectiveHidesManagedFields ? managedFieldsFilter.text : describeText"))
        XCTAssertTrue(describeSource.contains("Button(\"\\(t(.exportDescribe))...\", action: onExport)"))
        XCTAssertTrue(describeSource.contains("Button(\"Save Describe to Export Folder\", action: onExportToExportFolder)"))
        XCTAssertTrue(describeSource.contains("Button(\"Save Describe and Open\", action: onExportAndOpen)"))
        XCTAssertTrue(describeSource.contains("text: presentedDescribeText"))
    }

    func testDescribeManagedFieldsFilterRemovesOnlyManagedFieldsSection() {
        let source = """
        Name: api
        Namespace: default
        Managed Fields:
          API Version: v1
          Fields Type: FieldsV1
          fieldsV1:
            f:metadata:
              f:labels: {}
          Manager: kubectl
        Events:
          Type    Reason
        """

        let filtered = DescribeManagedFieldsDisplayFilter.removingManagedFields(from: source)

        XCTAssertEqual(filtered.removedBlockCount, 1)
        XCTAssertTrue(filtered.text.contains("Name: api"))
        XCTAssertTrue(filtered.text.contains("Events:"))
        XCTAssertFalse(filtered.text.contains("Managed Fields:"))
        XCTAssertFalse(filtered.text.contains("fieldsV1"))
        XCTAssertFalse(filtered.text.contains("Manager: kubectl"))
    }

    func testYAMLToolbarConsolidatesSecondaryActionsIntoMenus() throws {
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)
        let presentationSource = try String(contentsOfFile: resourceManifestPresentationPath, encoding: .utf8)

        XCTAssertEqual(yamlSource.occurrences(of: "ManifestYAMLActionMenus("), 2)
        XCTAssertTrue(presentationSource.contains("Label(draftTitle, systemImage: \"clock.arrow.circlepath\")"))
        XCTAssertTrue(presentationSource.contains("Button(\"Apply Last Fetched YAML\", action: onReapplySnapshot)"))
        XCTAssertFalse(presentationSource.contains("Button(\"Undo Draft Edit\", action: onUndoEdit)"))
        XCTAssertTrue(presentationSource.contains("Button(\"Revert Draft\", action: onRevert)"))
        XCTAssertTrue(presentationSource.contains("Label(fileTitle, systemImage: \"doc\")"))
        XCTAssertTrue(presentationSource.contains("Button(\"Import YAML…\", action: onImport)"))
        XCTAssertTrue(presentationSource.contains("Button(\"Export YAML…\", action: onExport)"))
        XCTAssertTrue(presentationSource.contains("Button(\"Save YAML to Export Folder\", action: onExportToExportFolder)"))
        XCTAssertTrue(presentationSource.contains("Button(\"Save YAML and Open\", action: onExportAndOpen)"))
        XCTAssertFalse(presentationSource.contains("Re-apply Snapshot"))
    }

    func testYAMLEditorsExposeDirectUndoWithOneActiveCommandShortcut() throws {
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)
        let presentationSource = try String(contentsOfFile: resourceManifestPresentationPath, encoding: .utf8)
        let rootSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let appKitSource = try String(contentsOfFile: appKitManifestTextViewPath, encoding: .utf8)
        let yamlPaneSource = try XCTUnwrap(yamlSource.slice(
            from: "struct ResourceYAMLInspectorPane",
            to: "struct ManifestUnsavedEditsChip"
        ))
        let yamlSheetSource = try XCTUnwrap(yamlSource.slice(
            from: "struct ResourceYAMLEditorSheetView",
            to: "private func navigateToIssue"
        ))
        let openSheetSource = try XCTUnwrap(rootSource.slice(
            from: "private func openYAMLEditorSheet()",
            to: "private func yamlManifestEditorSheet()"
        ))

        XCTAssertEqual(yamlSource.occurrences(of: "ManifestEditorUndoButton("), 2)
        XCTAssertTrue(yamlPaneSource.contains("if isInlineEditing {\n                    ManifestEditorUndoButton("))
        XCTAssertTrue(yamlSheetSource.contains("ManifestEditorUndoButton("))
        XCTAssertTrue(presentationSource.contains("Label(\"Undo\", systemImage: \"arrow.uturn.backward\")"))
        XCTAssertTrue(presentationSource.contains(".keyboardShortcut(\"z\", modifiers: [.command])"))
        XCTAssertTrue(presentationSource.contains(".disabled(!canUndo)"))
        XCTAssertTrue(presentationSource.contains("Button(\"Revert Draft\", action: onRevert)"))
        XCTAssertFalse(presentationSource.contains("Button(\"Undo Draft Edit\""))
        XCTAssertEqual(yamlSource.occurrences(of: "onUndoCommand: onUndoEdit"), 2)
        XCTAssertTrue(appKitSource.contains("if let onUndoCommand, shouldHandleUndoCommand(event)"))
        XCTAssertTrue(appKitSource.contains(
            "let disallowedModifiers: NSEvent.ModifierFlags = [.shift, .option, .control, .function]"
        ))
        XCTAssertTrue(openSheetSource.contains(
            "yamlManifestIsEditing = false\n        isYAMLEditorSheetPresented = true"
        ))
    }

    func testManifestEditorUndoButtonRoutesCommandZToDraftUndoAction() async throws {
        var undoCount = 0
        let host = NSHostingController(
            rootView: ManifestEditorUndoButton(
                canUndo: true,
                onUndo: { undoCount += 1 }
            )
            .frame(width: 180, height: 80)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 180, height: 80),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        let commandZ = try XCTUnwrap(NSEvent.keyEvent(
            with: .keyDown,
            location: .zero,
            modifierFlags: [.command],
            timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window.windowNumber,
            context: nil,
            characters: "z",
            charactersIgnoringModifiers: "z",
            isARepeat: false,
            keyCode: 6
        ))
        NSApp.sendEvent(commandZ)
        try await settle(window: window)

        XCTAssertEqual(undoCount, 1)
    }

    func testManifestEditorCommandZPrefersDraftUndoOverFocusedTextViewUndo() async throws {
        var yamlText = "kind: ConfigMap\n"
        var draftUndoCount = 0
        let textBinding = Binding(
            get: { yamlText },
            set: { yamlText = $0 }
        )
        let host = NSHostingController(
            rootView: VStack {
                ManifestEditorUndoButton(
                    canUndo: true,
                    onUndo: { draftUndoCount += 1 }
                )
                AppKitManifestTextView(
                    text: textBinding,
                    isEditable: true,
                    contentStyle: .yaml,
                    onUndoCommand: { draftUndoCount += 1 }
                )
            }
            .frame(width: 420, height: 240)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 420, height: 240),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        let textView = try XCTUnwrap(findTextScrollView(in: host.view)?.documentView as? NSTextView)
        let insertionRange = NSRange(location: (textView.string as NSString).length, length: 0)
        window.makeFirstResponder(textView)
        textView.insertText("metadata: {}\n", replacementRange: insertionRange)
        let editedText = textView.string

        XCTAssertTrue(window.firstResponder === textView)
        XCTAssertFalse(
            textView.undoManager?.canUndo == true,
            "State-backed YAML undo must not retain a second conflicting native history."
        )
        XCTAssertEqual(yamlText, editedText)

        let commandZ = try XCTUnwrap(NSEvent.keyEvent(
            with: .keyDown,
            location: .zero,
            modifierFlags: [.command],
            timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window.windowNumber,
            context: nil,
            characters: "z",
            charactersIgnoringModifiers: "z",
            isARepeat: false,
            keyCode: 6
        ))
        NSApp.sendEvent(commandZ)
        try await settle(window: window)

        XCTAssertEqual(draftUndoCount, 1)
        XCTAssertEqual(textView.string, editedText)
        XCTAssertFalse(
            textView.undoManager?.canUndo == true,
            "Command-Z must use only the state-backed YAML history."
        )

        let commandShiftZ = try XCTUnwrap(NSEvent.keyEvent(
            with: .keyDown,
            location: .zero,
            modifierFlags: [.command, .shift],
            timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window.windowNumber,
            context: nil,
            characters: "Z",
            charactersIgnoringModifiers: "z",
            isARepeat: false,
            keyCode: 6
        ))
        NSApp.sendEvent(commandShiftZ)
        try await settle(window: window)

        XCTAssertEqual(draftUndoCount, 1, "Command-Shift-Z must remain available to the native responder chain.")
    }

    func testManifestEditorCommandZRestoresCaretAndHorizontalViewport() async throws {
        let originalText = "prefix: " + String(repeating: "long-value-", count: 90)
        let state = RuneAppState()
        state.setResourceYAML(originalText)
        let host = NSHostingController(
            rootView: ManifestUndoRestorationHarness(state: state)
                .frame(width: 340, height: 180)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 340, height: 180),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        let scrollView = try XCTUnwrap(findTextScrollView(in: host.view))
        let textView = try XCTUnwrap(scrollView.documentView as? NSTextView)
        let insertionLocation = (originalText as NSString).length - 24
        let preEditSelection = NSRange(location: insertionLocation, length: 0)
        textView.setSelectedRange(preEditSelection)
        window.makeFirstResponder(textView)
        textView.scrollRangeToVisible(preEditSelection)
        scrollView.reflectScrolledClipView(scrollView.contentView)
        try await settle(window: window)

        let preEditOrigin = scrollView.contentView.bounds.origin
        XCTAssertGreaterThan(preEditOrigin.x, 0)

        textView.insertText("X", replacementRange: preEditSelection)
        try await settle(window: window)
        XCTAssertEqual(
            textView.selectedRange(),
            NSRange(location: insertionLocation + 1, length: 0)
        )

        let commandZ = try XCTUnwrap(NSEvent.keyEvent(
            with: .keyDown,
            location: .zero,
            modifierFlags: [.command],
            timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window.windowNumber,
            context: nil,
            characters: "z",
            charactersIgnoringModifiers: "z",
            isARepeat: false,
            keyCode: 6
        ))
        NSApp.sendEvent(commandZ)
        try await settle(window: window)

        XCTAssertEqual(state.resourceYAML, originalText)
        XCTAssertEqual(textView.string, originalText)
        XCTAssertEqual(
            textView.selectedRange(),
            preEditSelection,
            "Draft undo must restore the insertion point from before the edit."
        )
        XCTAssertEqual(
            scrollView.contentView.bounds.origin.x,
            preEditOrigin.x,
            accuracy: 1,
            "Draft undo must restore the long-line horizontal viewport."
        )
    }

    func testReadOnlyManifestIgnoresCommandZ() async throws {
        var undoCount = 0
        let source = "kind: ConfigMap\n"
        let host = NSHostingController(
            rootView: AppKitManifestTextView(
                text: .constant(source),
                isEditable: false,
                contentStyle: .yaml,
                onUndoCommand: { undoCount += 1 }
            )
            .frame(width: 340, height: 180)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 340, height: 180),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)
        let textView = try XCTUnwrap(
            findTextScrollView(in: host.view)?.documentView as? NSTextView
        )
        window.makeFirstResponder(textView)
        let commandZ = try XCTUnwrap(NSEvent.keyEvent(
            with: .keyDown,
            location: .zero,
            modifierFlags: [.command],
            timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window.windowNumber,
            context: nil,
            characters: "z",
            charactersIgnoringModifiers: "z",
            isARepeat: false,
            keyCode: 6
        ))
        NSApp.sendEvent(commandZ)
        try await settle(window: window)

        XCTAssertEqual(undoCount, 0)
        XCTAssertEqual(textView.string, source)
    }

    func testToolbarUndoRestoresPresentationAfterEditorRemount() async throws {
        let originalText = (0..<180)
            .map { line in
                line == 140
                    ? "target: " + String(repeating: "long-value-", count: 100)
                    : "line-\(line): synthetic"
            }
            .joined(separator: "\n")
        let state = RuneAppState()
        state.setResourceYAML(originalText)
        let mount = ManifestUndoMountModel()
        let host = NSHostingController(
            rootView: ManifestUndoLifecycleHarness(state: state, mount: mount)
                .frame(width: 360, height: 220)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 360, height: 220),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        let originalTextView = try XCTUnwrap(
            findTextScrollView(in: host.view)?.documentView as? NSTextView
        )
        let targetRange = (originalText as NSString).range(of: "target: ")
        let insertionLocation = targetRange.location + targetRange.length + 700
        let preEditSelection = NSRange(location: insertionLocation, length: 0)
        originalTextView.setSelectedRange(preEditSelection)
        window.makeFirstResponder(originalTextView)
        originalTextView.scrollRangeToVisible(preEditSelection)
        let originalScrollView = try XCTUnwrap(originalTextView.enclosingScrollView)
        originalScrollView.reflectScrolledClipView(originalScrollView.contentView)
        try await settle(window: window)
        let preEditOrigin = originalScrollView.contentView.bounds.origin

        XCTAssertGreaterThan(preEditOrigin.x, 0)
        XCTAssertGreaterThan(preEditOrigin.y, 0)

        originalTextView.insertText("X", replacementRange: preEditSelection)
        try await settle(window: window)
        XCTAssertTrue(state.canUndoResourceYAMLEdit)

        mount.editorID = UUID()
        try await settle(window: window)
        let remountedTextView = try XCTUnwrap(
            findTextScrollView(in: host.view)?.documentView as? NSTextView
        )
        XCTAssertFalse(remountedTextView === originalTextView)

        state.undoResourceYAMLEdit()
        try await settle(window: window)

        let remountedScrollView = try XCTUnwrap(remountedTextView.enclosingScrollView)
        XCTAssertEqual(remountedTextView.string, originalText)
        XCTAssertEqual(remountedTextView.selectedRange(), preEditSelection)
        XCTAssertEqual(
            remountedScrollView.contentView.bounds.origin.x,
            preEditOrigin.x,
            accuracy: 1
        )
        XCTAssertEqual(
            remountedScrollView.contentView.bounds.origin.y,
            preEditOrigin.y,
            accuracy: 1
        )
        XCTAssertTrue(window.firstResponder === remountedTextView)
    }

    func testInlineSheetAndDescribeShareManifestActionToolbarConstruction() throws {
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)
        let describeSource = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)

        XCTAssertEqual(yamlSource.occurrences(of: "ManifestActionToolbar("), 2)
        XCTAssertEqual(describeSource.occurrences(of: "ManifestActionToolbar("), 1)
        XCTAssertFalse(yamlSource.contains("ScrollView(.horizontal"))
        XCTAssertFalse(describeSource.contains("ScrollView(.horizontal"))
    }

    func testServerDryRunControlExistsOnlyInFullYAMLEditorSheet() throws {
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)
        let rootSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let quickEditPane = try XCTUnwrap(yamlSource.slice(
            from: "struct ResourceYAMLInspectorPane",
            to: "struct ManifestUnsavedEditsChip"
        ))
        let fullEditorSheet = try XCTUnwrap(yamlSource.slice(
            from: "struct ResourceYAMLEditorSheetView",
            to: "private func navigateToIssue"
        ))
        let fullEditorWiring = try XCTUnwrap(rootSource.slice(
            from: "private func yamlManifestEditorSheet()",
            to: "private var yamlBlock"
        ))
        let quickEditWiring = try XCTUnwrap(rootSource.slice(
            from: "private var yamlBlock",
            to: "/// One pane at a time"
        ))

        XCTAssertTrue(fullEditorSheet.contains("let onDryRun: () -> Void"))
        XCTAssertTrue(fullEditorSheet.contains("Label(\"Dry Run\", systemImage: \"checkmark.shield\")"))
        XCTAssertTrue(fullEditorSheet.contains(".disabled(!canDryRun || isRunningDryRun)"))
        XCTAssertTrue(fullEditorSheet.contains(
            ".accessibilityIdentifier(\"resource-yaml-server-dry-run\")"
        ))
        XCTAssertTrue(fullEditorSheet.contains("statusText: dryRunStatus"))
        XCTAssertTrue(fullEditorSheet.contains(
            "Dry Run sends this draft to Kubernetes for validation but never applies it."
        ))
        XCTAssertTrue(fullEditorWiring.contains(
            "canDryRun: viewModel.canDryRunSelectedResourceYAML"
        ))
        XCTAssertTrue(fullEditorWiring.contains(
            "onDryRun: { viewModel.requestDryRunSelectedResourceYAML() }"
        ))

        XCTAssertFalse(quickEditPane.contains("onDryRun"))
        XCTAssertFalse(quickEditPane.contains("resource-yaml-server-dry-run"))
        XCTAssertFalse(quickEditPane.contains("Label(\"Dry Run\""))
        XCTAssertFalse(quickEditWiring.contains("requestDryRunSelectedResourceYAML"))
        XCTAssertFalse(quickEditWiring.contains("resourceYAMLDryRunStatus"))
    }

    func testManifestDocumentSurfacesUseTypedOverlaysInsteadOfStateMessagesAsText() throws {
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)
        let describeSource = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)
        let presentationSource = try String(contentsOfFile: resourceManifestPresentationPath, encoding: .utf8)

        XCTAssertTrue(yamlSource.contains("ManifestDocumentSurface(state: resolvedDocumentState)"))
        XCTAssertTrue(describeSource.contains("ManifestDocumentSurface(state: documentState)"))
        XCTAssertTrue(presentationSource.contains("RuneContentStateView(contentState)"))
        XCTAssertTrue(presentationSource.contains(".accessibilityHidden(state.contentState != nil)"))
        XCTAssertFalse(yamlSource.contains("Text(yamlFooterText)"))
    }

    func testRootPassesRawManifestBuffersWithExplicitDocumentStates() throws {
        let rootSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(rootSource.contains("private var yamlManifestDocumentState: ManifestDocumentState"))
        XCTAssertTrue(rootSource.contains("private var describeManifestDocumentState: ManifestDocumentState"))
        XCTAssertTrue(rootSource.contains("yamlDisplayText: viewModel.state.resourceYAML"))
        XCTAssertTrue(rootSource.contains("describeText: viewModel.state.resourceDescribe"))
        XCTAssertEqual(rootSource.occurrences(of: "documentState: yamlManifestDocumentState"), 2)
        XCTAssertEqual(rootSource.occurrences(of: "documentState: describeManifestDocumentState"), 1)
        XCTAssertFalse(rootSource.contains("return \"Loading YAML for"))
        XCTAssertFalse(rootSource.contains("return \"Loading describe output for"))
    }

    @MainActor
    func testManifestToolbarGroupsUseCompactActionAndTallerSourceGeometry() {
        let actionGroup = NSHostingView(
            rootView: ManifestToolbarGroup(role: .action) {
                Button("Apply") {}
            }
        )
        let sourceGroup = NSHostingView(
            rootView: ManifestToolbarGroup(role: .source) {
                Picker("Namespace", selection: .constant("sample")) {
                    Text("sample").tag("sample")
                }
            }
        )

        actionGroup.layoutSubtreeIfNeeded()
        sourceGroup.layoutSubtreeIfNeeded()

        XCTAssertEqual(
            actionGroup.fittingSize.height,
            RuneUILayoutMetrics.inspectorToolbarActionGroupHeight,
            accuracy: 0.5
        )
        XCTAssertEqual(
            sourceGroup.fittingSize.height,
            RuneUILayoutMetrics.inspectorToolbarSourceGroupHeight,
            accuracy: 0.5
        )
        XCTAssertLessThan(actionGroup.fittingSize.height, sourceGroup.fittingSize.height)
    }

    @MainActor
    func testManifestDocumentLoadingOverlayKeepsStatusOutOfEditorText() async throws {
        let editorText = Binding.constant("")
        let host = NSHostingController(
            rootView: ManifestDocumentSurface(
                state: .loading(
                    title: "Loading manifest",
                    message: "Retrieving the selected resource."
                )
            ) {
                ResourceYAMLEditorSurface(
                    text: editorText,
                    displayText: "",
                    readOnlyResetID: "typed-state-test",
                    inlineEditing: false,
                    implementation: .appKitTextView,
                    validationIssues: [],
                    navigationRequest: nil,
                    searchQuery: "",
                    searchMatchCase: false,
                    selectedSearchMatchIndex: 0
                )
            }
            .frame(width: 520, height: 320)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 520, height: 320),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        let textView = try XCTUnwrap(findTextScrollView(in: host.view)?.documentView as? NSTextView)
        XCTAssertEqual(textView.string, "")
        XCTAssertFalse(textView.string.contains("Loading manifest"))
        XCTAssertFalse(textView.string.contains("Retrieving the selected resource"))
    }

    @MainActor
    func testYAMLSheetValidationIssuesStayInsideBoundedScrollableRegion() {
        let issues = (1...24).map { index in
            YAMLValidationIssue(
                source: .kubernetes,
                severity: index.isMultiple(of: 2) ? .warning : .error,
                message: "Synthetic validation issue \(index) with enough detail to wrap safely",
                line: index,
                column: 3
            )
        }
        let host = NSHostingView(
            rootView: YAMLValidationSummaryView(
                issues: issues,
                isValidating: false,
                expandedListMaxHeight: RuneUILayoutMetrics.yamlSheetValidationListMaxHeight,
                initiallyExpanded: true,
                onSelectIssue: { _ in }
            )
            .frame(width: 520)
        )

        host.layoutSubtreeIfNeeded()

        XCTAssertLessThanOrEqual(
            host.fittingSize.height,
            RuneUILayoutMetrics.yamlSheetValidationListMaxHeight + 70
        )
        XCTAssertTrue(containsScrollView(in: host))
    }

    func testManagedFieldsDisplayFilterRemovesNestedBlocksOnlyForDisplay() {
        let source = """
        apiVersion: v1
        kind: Pod
        metadata:
          name: api
          managedFields:
          - apiVersion: v1
            fieldsType: FieldsV1
            fieldsV1:
              f:metadata:
                f:labels: {}
          labels:
            app: api
        spec:
          containers: []
        """

        let filtered = KubernetesManagedFieldsDisplayFilter.removingManagedFields(from: source)

        XCTAssertEqual(filtered.removedBlockCount, 1)
        XCTAssertFalse(filtered.text.contains("managedFields"))
        XCTAssertFalse(filtered.text.contains("fieldsV1"))
        XCTAssertTrue(filtered.text.contains("  labels:"))
        XCTAssertTrue(filtered.text.contains("spec:"))
    }

    func testYAMLLineNumberGutterMetricsScaleWithFontAndLineCount() {
        let font = NSFont.monospacedSystemFont(ofSize: 13, weight: .regular)

        let twoDigitMetrics = YAMLLineNumberGutterMetrics(font: font, lineCount: 45)
        let fourDigitMetrics = YAMLLineNumberGutterMetrics(font: font, lineCount: 1_200)

        XCTAssertGreaterThan(fourDigitMetrics.gutterWidth, twoDigitMetrics.gutterWidth)
        XCTAssertGreaterThan(twoDigitMetrics.textInset, twoDigitMetrics.gutterWidth)
        XCTAssertGreaterThan(twoDigitMetrics.trailingPadding, twoDigitMetrics.leadingPadding)
        XCTAssertEqual(
            twoDigitMetrics.gutterWidth,
            ceil(twoDigitMetrics.leadingPadding + twoDigitMetrics.numberColumnWidth + twoDigitMetrics.trailingPadding),
            accuracy: 0.001
        )
    }

    func testAppKitYAMLTextViewReservesIntegratedLineNumberGutter() async throws {
        let text = Binding.constant("apiVersion: v1\nkind: Pod\nmetadata:\n  name: api\n")
        let host = NSHostingController(
            rootView: AppKitManifestTextView(
                text: text,
                isEditable: true,
                contentStyle: .yaml,
                showsLineNumbers: true
            )
            .frame(width: 460, height: 260)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 460, height: 260),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        guard let scrollView = findTextScrollView(in: host.view) else {
            return XCTFail("Expected YAML NSTextView-backed scroll view")
        }

        let manifestScrollView = try XCTUnwrap(scrollView as? ManifestTextScrollView)
        XCTAssertFalse(manifestScrollView.lineNumberGutterView.isHidden)
        XCTAssertTrue(manifestScrollView.lineNumberGutterView.isOpaque)
        XCTAssertEqual(manifestScrollView.lineNumberGutterView.gutterBackgroundColor.alphaComponent, 1, accuracy: 0.001)
        XCTAssertGreaterThan(manifestScrollView.lineNumberGutterView.frame.width, 20)
        XCTAssertLessThan(manifestScrollView.lineNumberGutterView.frame.width, 48)
        XCTAssertEqual(
            Array(manifestScrollView.lineNumberGutterView.visibleLineNumberLabels().map { $0.number }.prefix(4)),
            [1, 2, 3, 4]
        )

        let textView = try XCTUnwrap(scrollView.documentView as? NSTextView)
        let font = try XCTUnwrap(textView.font)
        let expectedMetrics = YAMLLineNumberGutterMetrics(font: font, lineCount: 5)

        XCTAssertFalse(scrollView.hasVerticalRuler)
        XCTAssertFalse(scrollView.rulersVisible)
        XCTAssertNil(scrollView.verticalRulerView)
        XCTAssertEqual(scrollView.contentInsets.left, 0, accuracy: 0.001)
        XCTAssertGreaterThanOrEqual(textView.textContainerInset.width, expectedMetrics.textInset - 0.5)
        XCTAssertGreaterThan(textView.textContainerInset.width, expectedMetrics.gutterWidth)
        XCTAssertLessThan(textView.textContainerInset.width, 48)

        let layoutManager = try XCTUnwrap(textView.layoutManager)
        let textContainer = try XCTUnwrap(textView.textContainer)
        layoutManager.ensureLayout(for: textContainer)
        let firstGlyphRect = layoutManager.boundingRect(forGlyphRange: NSRange(location: 0, length: 1), in: textContainer)
        let firstGlyphXInDocument = textView.textContainerOrigin.x + firstGlyphRect.minX

        XCTAssertGreaterThanOrEqual(
            firstGlyphXInDocument,
            expectedMetrics.gutterWidth + expectedMetrics.textPadding - 1,
            "YAML text should start after the integrated line-number gutter, with right-side padding reserved."
        )
    }

    func testAppKitYAMLTextViewExpandsLineNumberGutterWhenLineCountGrows() async throws {
        var yamlText = "apiVersion: v1\nkind: Pod\n"
        let binding = Binding(
            get: { yamlText },
            set: { yamlText = $0 }
        )
        let host = NSHostingController(
            rootView: AppKitManifestTextView(
                text: binding,
                isEditable: true,
                contentStyle: .yaml,
                showsLineNumbers: true
            )
            .frame(width: 460, height: 260)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 460, height: 260),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        let initialTextView = try XCTUnwrap(findTextScrollView(in: host.view)?.documentView as? NSTextView)
        let initialInset = initialTextView.textContainerInset.width

        yamlText = (0..<1_200)
            .map { "key\($0): value" }
            .joined(separator: "\n")
        host.rootView = AppKitManifestTextView(
            text: binding,
            isEditable: true,
            contentStyle: .yaml,
            showsLineNumbers: true
        )
        .frame(width: 460, height: 260)

        try await settle(window: window)

        let updatedTextView = try XCTUnwrap(findTextScrollView(in: host.view)?.documentView as? NSTextView)
        XCTAssertGreaterThan(
            updatedTextView.textContainerInset.width,
            initialInset,
            "Line-number gutter should grow from the document line count, not use a fixed width."
        )
    }

    func testDescribePaneAllowsScrollingLongDescribeOutput() async throws {
        let describeText = makeLongDescribeText()
        let (host, window) = makeDescribeHost(describeText: describeText)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        guard let scrollView = findTextScrollView(in: host.view),
              let textView = scrollView.documentView as? NSTextView,
              let textContainer = textView.textContainer,
              let layoutManager = textView.layoutManager
        else {
            return XCTFail("Expected describe NSTextView-backed scroll view")
        }

        layoutManager.ensureLayout(for: textContainer)
        let usedHeight = layoutManager.usedRect(for: textContainer).height + textView.textContainerInset.height * 2
        let viewportHeight = scrollView.contentView.bounds.height
        let documentHeight = scrollView.documentView?.frame.height ?? 0

        XCTAssertGreaterThan(usedHeight, viewportHeight + 200, "Describe content should overflow vertically")
        XCTAssertGreaterThan(documentHeight, viewportHeight + 200, "Describe scroll view document should be taller than the viewport")

        let targetOffset = min(240, max(40, usedHeight - viewportHeight - 20))
        scrollView.contentView.scroll(to: NSPoint(x: 0, y: targetOffset))
        scrollView.reflectScrolledClipView(scrollView.contentView)

        try await settle(window: window)

        XCTAssertGreaterThan(scrollView.contentView.bounds.origin.y, 0, "Describe pane should scroll downward")
    }

    func testDescribeTextSurfaceStaysInsideInspectorBounds() async throws {
        let (host, window) = makeDescribeHost(describeText: makeLongDescribeText())
        defer { window.orderOut(nil) }

        try await settle(window: window)

        guard let scrollView = findTextScrollView(in: host.view) else {
            return XCTFail("Expected describe NSTextView-backed scroll view")
        }

        let scrollFrame = scrollView.convert(scrollView.bounds, to: host.view)
        let hostBounds = host.view.bounds

        XCTAssertGreaterThanOrEqual(scrollFrame.minX, hostBounds.minX - 1, "Describe text surface should not render left of its inspector")
        XCTAssertGreaterThanOrEqual(scrollFrame.minY, hostBounds.minY - 1, "Describe text surface should not render below its inspector")
        XCTAssertLessThanOrEqual(scrollFrame.maxX, hostBounds.maxX + 1, "Describe text surface should not render right of its inspector")
        XCTAssertLessThanOrEqual(scrollFrame.maxY, hostBounds.maxY + 1, "Describe text surface should not render above its inspector")
    }

    func testLargeDescribeSearchMovesNextResultIntoVirtualizedViewport() async throws {
        let lineCount = 1_400
        let payload = String(repeating: " synthetic-field", count: 15)
        let text = (0..<lineCount)
            .map { index in
                let marker = index == 15 || index == 1_180 ? " needle" : ""
                return "Field-\(index): value\(marker)\(payload)"
            }
            .joined(separator: "\n")
        XCTAssertGreaterThan(text.utf8.count, 250_000)

        let model = LargeInspectorFindModel(text: text)
        let host = NSHostingController(
            rootView: LargeInspectorFindHarness(model: model)
                .frame(width: 720, height: 420)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 720, height: 420),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)
        try await settle(window: window)
        model.selectedMatchIndex = 1
        try await Task.sleep(nanoseconds: 400_000_000)
        try await settle(window: window)

        let scrollView = try XCTUnwrap(
            findScrollViews(in: host.view).max {
                ($0.documentView?.bounds.height ?? 0) < ($1.documentView?.bounds.height ?? 0)
            }
        )
        let documentHeight = try XCTUnwrap(scrollView.documentView?.bounds.height)
        let estimatedRowHeight = documentHeight / CGFloat(lineCount)
        let targetMidY = CGFloat(1_180) * estimatedRowHeight + estimatedRowHeight / 2
        let visibleMidY = scrollView.contentView.bounds.midY

        XCTAssertGreaterThan(scrollView.contentView.bounds.minY, documentHeight * 0.6)
        XCTAssertLessThan(
            (targetMidY - visibleMidY).magnitude,
            max(estimatedRowHeight * 5, scrollView.contentView.bounds.height * 0.22)
        )
    }

    private func makeDescribeHost(describeText: String) -> (NSHostingController<some View>, NSWindow) {
        let host = NSHostingController(
            rootView: ResourceDescribeInspectorPane(
                describeText: describeText,
                resourceReference: "pod api-0",
                canApplyMutations: true,
                yamlText: "apiVersion: v1\nkind: Pod\n",
                hasUnsavedEdits: true,
                validationIssues: [],
                statusText: "Last updated 12:00:00",
                onApply: {},
                onOpenYAMLEditor: {},
                onExport: {},
                onExportToExportFolder: {},
                onExportAndOpen: {},
                readOnlyResetID: "describe-scroll-test"
            )
            .frame(width: 640, height: 520)
        )

        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 640, height: 520),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        return (host, window)
    }

    private func settle(window: NSWindow) async throws {
        for _ in 0..<5 {
            window.contentView?.layoutSubtreeIfNeeded()
            hostRunLoopTick()
            try await Task.sleep(nanoseconds: 30_000_000)
        }
    }

    private func hostRunLoopTick() {
        RunLoop.current.run(until: Date().addingTimeInterval(0.01))
    }

    private func findTextScrollView(in view: NSView) -> NSScrollView? {
        if let scrollView = view as? NSScrollView,
           scrollView.documentView is NSTextView {
            return scrollView
        }

        for subview in view.subviews {
            if let match = findTextScrollView(in: subview) {
                return match
            }
        }

        return nil
    }

    private func findScrollViews(in view: NSView) -> [NSScrollView] {
        var matches: [NSScrollView] = []
        if let scrollView = view as? NSScrollView {
            matches.append(scrollView)
        }
        for subview in view.subviews {
            matches.append(contentsOf: findScrollViews(in: subview))
        }
        return matches
    }

    private func containsScrollView(in view: NSView) -> Bool {
        if view is NSScrollView {
            return true
        }
        return view.subviews.contains(where: containsScrollView)
    }

    private func makeLongDescribeText() -> String {
        (1...220).map { index in
            """
            Name: pod-api-\(index)
            Namespace: backend
            Status: Running
            Containers:
              api:
                Image: company/api:\(index)
                Args:
                  --feature-flag=enabled
                  --replica=\(index)
            """
        }
        .joined(separator: "\n\n")
    }

    private var describeTextViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/DescribeTextView.swift").path
    }

    private var resourceDescribeInspectorViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceDescribeInspectorView.swift").path
    }

    private var resourceYAMLInspectorViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceYAMLInspectorView.swift").path
    }

    private var resourceManifestInspectorLayoutPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceManifestInspectorLayout.swift").path
    }

    private var resourceManifestPresentationPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceManifestPresentation.swift").path
    }

    private var runeRootViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift").path
    }

    private var inspectorTextViewsPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/InspectorTextViews.swift").path
    }

    private var inspectorFindPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/InspectorFind.swift").path
    }

    private var appKitManifestTextViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/AppKitManifestTextView.swift").path
    }
}

@MainActor
private final class LargeInspectorFindModel: ObservableObject {
    let text: String
    @Published var query = "needle"
    @Published var matchCase = false
    @Published var selectedMatchIndex = 0
    @Published var isPresented = true

    init(text: String) {
        self.text = text
    }
}

private struct LargeInspectorFindHarness: View {
    @ObservedObject var model: LargeInspectorFindModel

    var body: some View {
        FindableInspectorSurface(
            text: model.text,
            placeholder: "Find in describe",
            query: $model.query,
            matchCase: $model.matchCase,
            selectedMatchIndex: $model.selectedMatchIndex,
            isFindPresented: $model.isPresented
        ) { searchIndex, searchNavigationRevision in
            DescribeTextSurface(
                text: model.text,
                minHeight: 280,
                resetID: "large-describe-search",
                searchQuery: model.query,
                searchMatchCase: model.matchCase,
                selectedSearchMatchIndex: model.selectedMatchIndex,
                searchIndex: searchIndex,
                searchNavigationRevision: searchNavigationRevision
            )
        }
    }
}

@MainActor
private final class EditableInspectorFindModel: ObservableObject {
    @Published var text: String
    @Published var query: String
    @Published var matchCase = false
    @Published var selectedMatchIndex = 0
    @Published var isPresented = true

    init(text: String, query: String = "needle") {
        self.text = text
        self.query = query
    }
}

private struct EditableInspectorFindHarness: View {
    @ObservedObject var model: EditableInspectorFindModel

    var body: some View {
        FindableInspectorSurface(
            text: model.text,
            placeholder: "Find in YAML",
            query: $model.query,
            matchCase: $model.matchCase,
            selectedMatchIndex: $model.selectedMatchIndex,
            isFindPresented: $model.isPresented
        ) { searchIndex, searchNavigationRevision in
            AppKitManifestTextView(
                text: $model.text,
                isEditable: true,
                contentStyle: .yaml,
                searchQuery: model.query,
                searchMatchCase: model.matchCase,
                selectedSearchMatchIndex: model.selectedMatchIndex,
                searchMatchRanges: searchIndex?.ranges ?? [],
                searchNavigationRevision: searchNavigationRevision
            )
        }
    }
}

private struct ManifestUndoRestorationHarness: View {
    @ObservedObject var state: RuneAppState

    var body: some View {
        let searchRange = (state.resourceYAML as NSString).range(of: "prefix")
        AppKitManifestTextView(
            text: Binding(
                get: { state.resourceYAML },
                set: { state.updateResourceYAMLDraft($0) }
            ),
            isEditable: true,
            contentStyle: .yaml,
            showsLineNumbers: true,
            searchQuery: "prefix",
            searchMatchCase: false,
            selectedSearchMatchIndex: 0,
            searchMatchRanges: searchRange.location == NSNotFound ? [] : [searchRange],
            searchNavigationRevision: 1,
            editorRestorationRequest: state.resourceYAMLEditorRestorationRequest,
            onEditorEdit: { yaml, presentation in
                state.updateResourceYAMLDraft(
                    yaml,
                    undoPresentation: presentation
                )
            },
            onUndoCommand: state.undoResourceYAMLEdit
        )
    }
}

@MainActor
private final class ManifestUndoMountModel: ObservableObject {
    @Published var editorID = UUID()
}

private struct ManifestUndoLifecycleHarness: View {
    @ObservedObject var state: RuneAppState
    @ObservedObject var mount: ManifestUndoMountModel

    var body: some View {
        AppKitManifestTextView(
            text: Binding(
                get: { state.resourceYAML },
                set: { state.updateResourceYAMLDraft($0) }
            ),
            isEditable: true,
            contentStyle: .yaml,
            showsLineNumbers: true,
            editorRestorationRequest: state.resourceYAMLEditorRestorationRequest,
            onEditorEdit: { yaml, presentation in
                state.updateResourceYAMLDraft(
                    yaml,
                    undoPresentation: presentation
                )
            },
            onUndoCommand: state.undoResourceYAMLEdit
        )
        .id(mount.editorID)
    }
}

private extension String {
    func occurrences(of substring: String) -> Int {
        guard !substring.isEmpty else { return 0 }
        var count = 0
        var searchRange = startIndex..<endIndex

        while let match = range(of: substring, range: searchRange) {
            count += 1
            searchRange = match.upperBound..<endIndex
        }

        return count
    }

    func slice(from start: String, to end: String) -> String? {
        guard let startRange = range(of: start),
              let endRange = range(of: end, range: startRange.upperBound..<endIndex) else {
            return nil
        }
        return String(self[startRange.lowerBound..<endRange.lowerBound])
    }
}
