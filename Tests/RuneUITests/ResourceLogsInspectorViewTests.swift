import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

final class ResourceLogsInspectorViewTests: XCTestCase {
    func testLogToolbarUsesStableRowsInsteadOfBreakpointWrapping() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let toolbarSource = try XCTUnwrap(source.slice(
            from: "struct ResourceLogsToolbar: View",
            to: "private enum LogToolbarGroupRole"
        ))

        XCTAssertFalse(toolbarSource.contains("ViewThatFits(in: .horizontal)"))
        XCTAssertTrue(source.contains("LogToolbarScrollRow"))
        XCTAssertTrue(source.contains("private var sourceControls: some View"))
        XCTAssertTrue(source.contains("private var primaryControls: some View"))
        XCTAssertTrue(source.contains("private var toolbarActions: some View"))
        XCTAssertTrue(source.contains("enum ResourceLogsPresentationStyle"))
        XCTAssertTrue(source.contains("case terminalCompact"))
        XCTAssertTrue(source.contains("private var terminalCompactBody: some View"))
        XCTAssertTrue(source.contains("RuneUILayoutMetrics.inspectorCompactSectionSpacing"))
        XCTAssertTrue(source.contains("RuneUILayoutMetrics.inspectorSectionSpacing"))
        XCTAssertTrue(source.contains("LogToolbarGroup"))
        XCTAssertTrue(source.contains("LogToolbarGroup(role: .source)"))
        XCTAssertTrue(source.contains("LogToolbarGroup(spacing: 6)"))
        XCTAssertTrue(source.contains("LogToolbarPickerField(title: t(.window), role: .window)"))
        XCTAssertTrue(source.contains("LogToolbarPickerField(title: t(.container), role: .container)"))
        XCTAssertTrue(source.contains("static let sourcePickerWidth: CGFloat = 180"))
        XCTAssertEqual(
            source.components(separatedBy: ".frame(width: ResourceLogsLayoutMetrics.sourcePickerWidth)").count - 1,
            2
        )
        XCTAssertEqual(source.components(separatedBy: "LogToolbarPopupPicker(").count - 1, 2)
        XCTAssertTrue(source.contains("func makeNSView(context: Context) -> NSPopUpButton"))
        XCTAssertTrue(source.contains("LogToolbarStatusIndicator("))
        XCTAssertTrue(source.contains("private struct LogToolbarStatusIndicator"))
        XCTAssertTrue(source.contains("sourcePanelTitle: \"Pods\""))
        XCTAssertTrue(source.contains("LogToolbarSourceSummary("))
        XCTAssertTrue(source.contains("sourceSummaryTitle: source.sourcePanelTitle"))
        XCTAssertTrue(source.contains(".toggleStyle(.button)"))
        XCTAssertTrue(source.contains(".fixedSize(horizontal: true, vertical: false)"))
        XCTAssertTrue(source.contains(".frame(minHeight: role.height)"))
        XCTAssertTrue(source.contains("RuneUILayoutMetrics.inspectorToolbarSourceGroupHeight"))
        XCTAssertTrue(source.contains("RuneUILayoutMetrics.inspectorToolbarActionGroupHeight"))
        XCTAssertTrue(source.contains("RuneUILayoutMetrics.inspectorToolbarGroupSpacing"))
        XCTAssertTrue(source.contains("RuneUILayoutMetrics.inspectorToolbarControlSpacing"))
        XCTAssertTrue(source.contains("Label(t(.saveLogs), systemImage: \"square.and.arrow.down\")"))
        XCTAssertTrue(source.contains("Label(\"Current Logs\", systemImage: \"doc.text\")"))
        XCTAssertTrue(source.contains("Label(\"Save to Export Folder\", systemImage: \"folder\")"))
        XCTAssertTrue(source.contains("Label(\"Save and Open\", systemImage: \"arrow.up.right.square\")"))
        XCTAssertTrue(source.contains("Label(\"Save As...\", systemImage: \"doc.zipper\")"))
        XCTAssertTrue(source.contains("Label(\"Save As...\", systemImage: \"archivebox\")"))
        XCTAssertTrue(source.contains("Label(\"Save As...\", systemImage: \"shippingbox\")"))
        XCTAssertTrue(source.contains("Label(\"Save to Export Folder\", systemImage: \"folder.badge.plus\")"))
        XCTAssertTrue(source.contains("Label(\"Save and Open\", systemImage: \"archivebox\")"))
        XCTAssertTrue(source.contains(".buttonStyle(.borderedProminent)"))
        XCTAssertTrue(source.contains("Label(t(.more), systemImage: \"ellipsis.circle\")"))
        XCTAssertTrue(source.contains("t(.exportVisibleResultsZip)"))
        XCTAssertTrue(source.contains("t(.exportFullUnfilteredZip)"))
        XCTAssertTrue(source.contains("t(.exportAllPodsFullZip)"))
        XCTAssertTrue(source.contains("onSaveVisibleZip(visibleLogText)"))
        XCTAssertTrue(source.contains("onSaveVisibleZipToExportFolder(visibleLogText)"))
        XCTAssertTrue(source.contains("onSaveVisibleZipAndOpen(visibleLogText)"))
        XCTAssertTrue(source.contains("Button(action: onSaveFullZip)"))
        XCTAssertTrue(source.contains("Button(action: onSaveFullZipToExportFolder)"))
        XCTAssertTrue(source.contains("Button(action: onSaveFullZipAndOpen)"))
        XCTAssertTrue(source.contains("Button(action: onSaveAllPodsZip)"))
        XCTAssertTrue(source.contains("Button(action: onSaveAllPodsZipToExportFolder)"))
        XCTAssertTrue(source.contains("Button(action: onSaveAllPodsZipAndOpen)"))
        XCTAssertTrue(source.contains("t(.copySelection)"))
        XCTAssertTrue(source.contains(".controlSize(.small)"))
        XCTAssertTrue(source.contains(".logToolbarButtonFrame()"))
        XCTAssertTrue(source.contains(".logToolbarIconButtonFrame()"))
        XCTAssertTrue(source.contains("idealWidth: width"))
        XCTAssertFalse(source.contains("maxHeight: RuneUILayoutMetrics.inspectorToolbarControlMinHeight"))
        XCTAssertTrue(source.contains(".font(.caption.weight(.semibold))"))
    }

    func testPodAndUnifiedLogsShareOneStatefulPaneEngine() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let podPane = try XCTUnwrap(source.slice(
            from: "struct PodLogsInspectorPane: View",
            to: "struct UnifiedResourceLogsInspectorPane: View"
        ))
        let unifiedPane = try XCTUnwrap(source.slice(
            from: "struct UnifiedResourceLogsInspectorPane: View",
            to: "struct ResourceLogsOutputSurface: View"
        ))

        XCTAssertTrue(source.contains("private struct ResourceLogsInspectorPaneCore: View"))
        XCTAssertTrue(source.contains("private struct ResourceLogsPaneSourceAdapter"))
        XCTAssertTrue(source.contains("static func pod("))
        XCTAssertTrue(source.contains("static func unified(podNames: [String])"))
        XCTAssertTrue(podPane.contains("ResourceLogsInspectorPaneCore("))
        XCTAssertTrue(podPane.contains("source: .pod("))
        XCTAssertTrue(unifiedPane.contains("ResourceLogsInspectorPaneCore("))
        XCTAssertTrue(unifiedPane.contains("source: .unified(podNames: podNames)"))
        XCTAssertEqual(
            source.components(separatedBy: "@State private var searchQuery = \"\"").count - 1,
            1,
            "Pod and unified logs must not drift into separate search/navigation state engines."
        )
        XCTAssertEqual(
            source.components(separatedBy: "private func scheduleStructuredSummary(for text: String, debounced: Bool)").count - 1,
            1,
            "Structured-log analysis should be wired once and configured by the source adapter."
        )
    }

    func testLogToolbarStatusAndControlsUseGroupedNativeChrome() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let toolbarGroupSource = try XCTUnwrap(source.slice(
            from: "private struct LogToolbarGroup",
            to: "private struct LogToolbarScrollRow"
        ))

        XCTAssertTrue(source.contains("LogToolbarGroup"))
        XCTAssertTrue(source.contains("LogToolbarStatusIndicator"))
        XCTAssertTrue(source.contains("private struct LogToolbarSourceSummary"))
        XCTAssertTrue(source.contains("statusColor"))
        XCTAssertTrue(source.contains("@Environment(\\.runeThemePalette) private var runeThemePalette"))
        XCTAssertTrue(source.contains("RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)"))
        XCTAssertTrue(source.contains("LogToolbarPopupPicker("))
        XCTAssertTrue(source.contains("popup.font = parent.popupFont"))
        XCTAssertTrue(source.contains("RuneInterfaceTypography.appKitMenuFontSize("))
        XCTAssertTrue(toolbarGroupSource.contains("runeThemePalette?.row.opacity(0.42)"))
        XCTAssertTrue(toolbarGroupSource.contains("runeThemePalette?.stroke.opacity(0.28)"))
        XCTAssertFalse(toolbarGroupSource.contains("RoundedRectangle(cornerRadius: 9, style: .continuous)"))
        XCTAssertTrue(source.contains("VStack(alignment: .leading, spacing: 5)"))
        XCTAssertTrue(source.contains("Text(title.uppercased())"))
        XCTAssertFalse(source.contains("Circle()\\n                    .fill(statusText.lowercased()"))
    }

    func testAdjustedLogControlsConsumeResolvedThemeTokens() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let findControls = try String(contentsOfFile: runeFindBarControlsPath, encoding: .utf8)
        let searchBarSource = try XCTUnwrap(source.slice(
            from: "struct ResourceLogsSearchBar: View",
            to: "struct ResourceLogSearchResult"
        ))
        let insightsSource = try XCTUnwrap(source.slice(
            from: "struct ResourceStructuredLogSummaryPanel: View",
            to: "struct ResourceLogsPaneActions"
        ))
        let pickerFieldSource = try XCTUnwrap(source.slice(
            from: "private struct LogToolbarPickerField",
            to: "struct LogToolbarPopupPicker"
        ))
        let matchCaseStart = try XCTUnwrap(findControls.range(of: "struct RuneMatchCaseButton: View"))
        let matchCaseSource = String(findControls[matchCaseStart.lowerBound...])

        XCTAssertTrue(searchBarSource.contains("@Environment(\\.runeThemePalette) private var runeThemePalette"))
        XCTAssertTrue(searchBarSource.contains("RuneSurfaceBackground(kind: .editor)"))
        XCTAssertTrue(searchBarSource.contains("runeThemePalette?.divider"))
        XCTAssertTrue(searchBarSource.contains("runeThemePalette?.secondaryText"))
        XCTAssertTrue(searchBarSource.contains("runeThemePalette?.foreground"))
        XCTAssertTrue(searchBarSource.contains("runeThemePalette?.mutedText"))
        XCTAssertTrue(searchBarSource.contains("runeThemePalette?.accent"))
        XCTAssertTrue(searchBarSource.contains("RuneSemanticColorRole.danger.color(in: runeThemePalette)"))
        XCTAssertFalse(searchBarSource.contains(".regularMaterial"))

        XCTAssertTrue(source.contains(".background(RuneSurfaceBackground(kind: .inset))"))
        XCTAssertTrue(insightsSource.contains("@Environment(\\.runeThemePalette) private var runeThemePalette"))
        XCTAssertTrue(insightsSource.contains("runeThemePalette?.foreground"))
        XCTAssertTrue(insightsSource.contains("runeThemePalette?.secondaryText"))
        XCTAssertTrue(insightsSource.contains("RuneChip("))
        XCTAssertTrue(pickerFieldSource.contains("@Environment(\\.runeThemePalette) private var runeThemePalette"))
        XCTAssertTrue(pickerFieldSource.contains("runeThemePalette?.secondaryText"))
        XCTAssertTrue(matchCaseSource.contains("@Environment(\\.runeThemePalette) private var runeThemePalette"))
        XCTAssertTrue(matchCaseSource.contains("runeThemePalette?.accent"))
        XCTAssertTrue(matchCaseSource.contains("runeThemePalette?.secondaryText"))
    }

    @MainActor
    func testAdjustedLogPanelRendersBuiltInAndDecodedCustomThemes() async throws {
        let customThemeData = Data(
            """
            {
              "name": "Synthetic Themes",
              "themes": [
                {
                  "name": "Synthetic Dark",
                  "appearance": "dark",
                  "style": {
                    "background": "#102030ff",
                    "panel.background": "#182838ff",
                    "element.background": "#213141ff",
                    "element.selected": "#345678ff",
                    "editor.background": "#0b1b2bff",
                    "border": "#6f8090ff",
                    "text": "#f4f8fbff",
                    "text.muted": "#b6c4d1ff",
                    "text.placeholder": "#8ea0adff",
                    "text.accent": "#ff4fb3ff",
                    "error": "#ff6677ff"
                  }
                }
              ]
            }
            """.utf8
        )
        let customTheme = try XCTUnwrap(
            RuneZedThemeDecoder.decode(data: customThemeData, sourceID: "synthetic-test").first
        )
        let model = ResourceLogAdjustedThemeModel(theme: RuneAppearanceTheme.paper.resolvedTheme)
        let host = NSHostingController(
            rootView: ResourceLogAdjustedThemeHarness(model: model)
                .frame(width: 700, height: 220, alignment: .topLeading)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 700, height: 220),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { closeTestWindow(window) }

        let themes = [
            RuneAppearanceTheme.paper.resolvedTheme,
            RuneAppearanceTheme.graphiteBlue.resolvedTheme,
            customTheme,
            RuneAppearanceTheme.paper.resolvedTheme
        ]
        var renderedImages: [Data] = []
        for (index, theme) in themes.enumerated() {
            model.theme = theme
            try await settle(window: window)
            let popups = findPopUpButtons(in: host.view)
            let sourcePopups = popups.filter {
                $0.accessibilityLabel() == "Log window" || $0.accessibilityLabel() == "Container"
            }
            XCTAssertEqual(sourcePopups.count, 2)
            let expectedAppearance: NSAppearance.Name = theme.preferredColorScheme == .light ? .aqua : .darkAqua
            for popup in sourcePopups {
                XCTAssertEqual(
                    popup.effectiveAppearance.bestMatch(from: [.aqua, .darkAqua]),
                    expectedAppearance,
                    "Popup appearance must follow theme at transition \(index)."
                )
            }
            let png = try renderedPNG(from: host.view)
            XCTAssertGreaterThan(png.count, 10_000)
            renderedImages.append(png)
            if let artifactDirectory = ProcessInfo.processInfo.environment["RUNE_UI_TEST_ARTIFACT_DIR"] {
                let directory = URL(fileURLWithPath: artifactDirectory, isDirectory: true)
                try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
                try png.write(to: directory.appendingPathComponent("logs-theme-\(index)-\(theme.id).png"))
            }
        }

        XCTAssertEqual(Set(renderedImages.prefix(3)).count, 3, "Paper, Graphite and custom palette must render distinctly.")
        XCTAssertEqual(renderedImages.first, renderedImages.last, "Returning to Paper must clear all custom-theme rendering.")
    }

    @MainActor
    func testLogSearchAccessoriesStayInlineAndContainedAtNestedInspectorWidths() async throws {
        let text = "INFO ready\nINFO retry\nERROR failed\n"
        let result = ResourceLogSearchResult.make(text: text, query: "INFO")

        for width in [CGFloat(248), 520] {
            let host = NSHostingController(
                rootView: ResourceLogsSearchBar(
                    query: .constant("INFO"),
                    matchCase: .constant(false),
                    selectedMatchIndex: .constant(0),
                    focusRequestID: 0,
                    searchSummary: result,
                    placeholder: "Search logs",
                    findHelp: "Find in logs",
                    matchCaseHelp: "Match case"
                )
                .frame(width: width, height: 60, alignment: .topLeading)
                .runeAppearanceTheme(RuneAppearanceTheme.graphiteBlue.resolvedTheme)
            )
            let window = NSWindow(
                contentRect: NSRect(x: 0, y: 0, width: width, height: 60),
                styleMask: [.borderless],
                backing: .buffered,
                defer: false
            )
            window.isReleasedWhenClosed = false
            window.contentViewController = host
            window.makeKeyAndOrderFront(nil)
            defer { closeTestWindow(window) }
            try await settle(window: window)

            let searchField = try XCTUnwrap(findEditableTextFields(in: host.view).first)
            let localSearchFrame = host.view.convert(searchField.bounds, from: searchField)
            let searchFrame = searchField.accessibilityFrame()
            XCTAssertGreaterThanOrEqual(searchFrame.minX, window.frame.minX - 0.5)
            XCTAssertLessThanOrEqual(searchFrame.maxX, window.frame.maxX + 0.5)
            XCTAssertGreaterThanOrEqual(searchFrame.width, width == 248 ? 100 : 160)

            let compactMenus = findPopUpButtons(in: host.view)
            if width == 248 {
                let optionsButton = try XCTUnwrap(compactMenus.only)
                let optionsFrame = host.view.convert(optionsButton.bounds, from: optionsButton)
                XCTAssertGreaterThanOrEqual(optionsFrame.minX, localSearchFrame.maxX - 1)
                XCTAssertLessThanOrEqual(optionsFrame.maxX, width + 0.5)
            } else {
                XCTAssertTrue(compactMenus.isEmpty, "The wide find row should expose direct Previous/Next actions.")
            }
            if let artifactDirectory = ProcessInfo.processInfo.environment["RUNE_UI_TEST_ARTIFACT_DIR"] {
                let directory = URL(fileURLWithPath: artifactDirectory, isDirectory: true)
                try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
                try renderedPNG(from: host.view).write(
                    to: directory.appendingPathComponent("logs-search-width-\(Int(width)).png")
                )
            }
        }
    }

    @MainActor
    func testLogWindowAndContainerPickersShareWidthHeightAndBaseline() async throws {
        let model = ResourceLogToolbarPickerModel()
        let host = NSHostingController(
            rootView: ResourceLogToolbarPickerHarness(model: model)
            .frame(width: 700, height: 140, alignment: .topLeading)
            .runeAppearanceTheme(RuneAppearanceTheme.graphiteBlue.resolvedTheme)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 700, height: 140),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { closeTestWindow(window) }

        try await settle(window: window)

        let popups = findPopUpButtons(in: host.view)
        let windowPopup = try XCTUnwrap(
            popups.first { $0.accessibilityLabel() == "Log window" },
            "Expected the explicitly labelled Window popup."
        )
        let containerPopup = try XCTUnwrap(
            popups.first { $0.accessibilityLabel() == "Container" },
            "Expected the explicitly labelled Container popup."
        )
        let windowFrame = host.view.convert(windowPopup.bounds, from: windowPopup)
        let containerFrame = host.view.convert(containerPopup.bounds, from: containerPopup)
        XCTAssertEqual(windowFrame.width, containerFrame.width, accuracy: 0.5)
        XCTAssertEqual(windowFrame.height, containerFrame.height, accuracy: 0.5)
        XCTAssertEqual(windowFrame.minY, containerFrame.minY, accuracy: 0.5)
        XCTAssertEqual(ResourceLogsLayoutMetrics.sourcePickerWidth, 180)
        XCTAssertEqual(ResourceLogsLayoutMetrics.podPickerWidth, 280)

        windowPopup.selectItem(at: 1)
        windowPopup.sendAction(windowPopup.action, to: windowPopup.target)
        containerPopup.selectItem(withTitle: "app")
        containerPopup.sendAction(containerPopup.action, to: containerPopup.target)
        try await settle(window: window)
        XCTAssertEqual(model.selectedLogPreset, .last5Minutes)
        XCTAssertEqual(model.selectedContainer, "app")

        model.selectedLogPreset = .lastHour
        model.selectedContainer = "sidecar"
        try await settle(window: window)
        XCTAssertEqual(windowPopup.titleOfSelectedItem, PodLogPreset.lastHour.title)
        XCTAssertEqual(containerPopup.titleOfSelectedItem, "sidecar")

        model.containerOptions = ["app"]
        try await settle(window: window)
        XCTAssertEqual(model.selectedContainer, "sidecar")
        XCTAssertEqual(containerPopup.indexOfSelectedItem, -1, "A stale binding must not display a different container.")
    }

    @MainActor
    func testNativeLogPopupTextActuallyTracksBoundedInterfaceFontScale() async throws {
        let configurations: [(
            configuredFontSize: Double,
            systemDynamicTypeSize: DynamicTypeSize,
            expectedOffset: CGFloat
        )] = [
            (12, .large, 0),
            (13, .large, 1),
            (15, .large, 2),
            (20, .xxxLarge, 3),
            (20, .accessibility3, 3)
        ]
        var observedPointSizes: [CGFloat] = []

        for configuration in configurations {
            let model = ResourceLogToolbarPickerModel()
            let host = NSHostingController(
                rootView: ResourceLogToolbarPickerHarness(model: model)
                    .runeInterfaceTypography(
                        configuredFontSize: configuration.configuredFontSize,
                        systemDynamicTypeSize: configuration.systemDynamicTypeSize
                    )
                    .frame(width: 700, height: 140, alignment: .topLeading)
                    .runeAppearanceTheme(RuneAppearanceTheme.graphiteBlue.resolvedTheme)
            )
            let window = NSWindow(
                contentRect: NSRect(x: 0, y: 0, width: 700, height: 140),
                styleMask: [.titled, .closable],
                backing: .buffered,
                defer: false
            )
            window.isReleasedWhenClosed = false
            window.contentViewController = host
            window.makeKeyAndOrderFront(nil)

            try await settle(window: window)

            let popups = findPopUpButtons(in: host.view).filter {
                $0.accessibilityLabel() == "Log window" || $0.accessibilityLabel() == "Container"
            }
            XCTAssertEqual(popups.count, 2)
            let expectedPointSize = NSFont.smallSystemFontSize + configuration.expectedOffset
            for popup in popups {
                XCTAssertFalse(popup.itemTitles.isEmpty)
                XCTAssertEqual(
                    try XCTUnwrap(popup.font).pointSize,
                    expectedPointSize,
                    accuracy: 0.01
                )
            }
            observedPointSizes.append(try XCTUnwrap(popups.first?.font).pointSize)
            closeTestWindow(window)
        }

        XCTAssertLessThan(observedPointSizes[0], observedPointSizes[1])
        XCTAssertLessThan(observedPointSizes[1], observedPointSizes[2])
        XCTAssertLessThan(observedPointSizes[2], observedPointSizes[3])
        XCTAssertEqual(observedPointSizes[3], observedPointSizes[4], accuracy: 0.01)
    }

    func testLogSearchRunsOffMainThreadAndKeepsStableInputIdentity() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let searchBarSource = try XCTUnwrap(source.slice(
            from: "struct ResourceLogsSearchBar: View",
            to: "struct ResourceLogSearchResult"
        ))

        XCTAssertTrue(source.contains("@State private var searchFocusRequestID = 0"))
        XCTAssertTrue(source.contains("applyLogSearchQuery"))
        XCTAssertTrue(source.contains("searchFocusRequestID: searchFocusRequestID"))
        XCTAssertEqual(searchBarSource.components(separatedBy: "TextField(").count - 1, 2)
        XCTAssertTrue(searchBarSource.contains("prompt: Text(placeholder)"))
        XCTAssertTrue(source.contains("@State private var searchTask: Task<Void, Never>?"))
        XCTAssertTrue(source.contains("querySearchDebounceNanoseconds: UInt64 = 25_000_000"))
        XCTAssertTrue(source.contains("streamedTextSearchDebounceNanoseconds: UInt64 = 0"))
        XCTAssertTrue(source.contains("streamedTextSummaryDebounceNanoseconds: UInt64 = 120_000_000"))
        XCTAssertTrue(source.contains("actor ResourceLogsLatestWorkLane<Output: Sendable>"))
        XCTAssertTrue(source.contains("Task.detached(priority: priority)"))
        XCTAssertTrue(source.contains("withTaskCancellationHandler"))
        XCTAssertTrue(source.contains("currentTask.cancel()"))
        XCTAssertTrue(source.contains("_ = await currentTask.result"))
        XCTAssertTrue(source.contains("cancellationCheck: { try Task.checkCancellation() }"))
        XCTAssertTrue(source.contains("let reusableTextIndex = searchResult.originalText == text ? searchResult.textIndex : nil"))
        XCTAssertTrue(source.contains("struct ResourceLogsExplorePanel"))
        XCTAssertFalse(source.contains("RuneFindBarChrome(\"Log find controls\")"))
        XCTAssertTrue(searchBarSource.contains("ViewThatFits(in: .horizontal)"))
        XCTAssertTrue(searchBarSource.contains("compactSearchAccessories"))
        XCTAssertFalse(searchBarSource.contains("LogSearchPulseOverlay"))
        XCTAssertFalse(searchBarSource.contains("flashSearchPulse"))
        XCTAssertFalse(searchBarSource.contains("Text(searchSummary.badgeText)"))
        XCTAssertFalse(searchBarSource.contains("Spacer(minLength:"))
        XCTAssertTrue(searchBarSource.contains("private func matchControls(statusWidth: CGFloat)"))
        XCTAssertTrue(searchBarSource.contains(".fixedSize(horizontal: true, vertical: false)"))
        XCTAssertTrue(searchBarSource.contains("ResourceLogsLayoutMetrics.searchMatchStatusWidth"))
        XCTAssertTrue(searchBarSource.contains("transaction.animation = nil"))
        XCTAssertTrue(source.contains("@FocusState private var isSearchFocused"))
        XCTAssertTrue(source.contains(".keyboardShortcut(\"f\", modifiers: [.command])"))
        XCTAssertTrue(source.contains("RuneMatchCaseButton(isSelected: $matchCase, help: matchCaseHelp)"))
        XCTAssertTrue(source.contains("searchResult.isRenderableSnapshot(for: logText)"))
        XCTAssertTrue(source.contains("renderSearchResult: renderSearchResult"))
        XCTAssertTrue(source.contains("navigationSearchResult: activeSearchResult"))
        XCTAssertTrue(source.contains("onSearchNavigate:"))
        XCTAssertTrue(searchBarSource.contains("onNavigate()"))
        XCTAssertTrue(source.contains("private func scheduleStructuredSummary(for text: String, debounced: Bool)"))
        XCTAssertFalse(source.contains(".task(id: \"\\(simpleMode):\\(logText)\")"))
    }

    func testLogSearchSupportsMatchCase() {
        let insensitive = ResourceLogSearchResult.make(
            text: "ERROR failed\nerror lower\n",
            query: "error",
            matchCase: false
        )
        let sensitive = ResourceLogSearchResult.make(
            text: "ERROR failed\nerror lower\n",
            query: "error",
            matchCase: true
        )

        XCTAssertEqual(insensitive.matchRanges.count, 2)
        XCTAssertEqual(sensitive.matchRanges.count, 1)
        XCTAssertFalse(insensitive.matchCase)
        XCTAssertTrue(sensitive.matchCase)
    }

    func testLogSearchReusesExistingTextIndexAcrossQueries() {
        let text = "INFO ready\nERROR failed\n"
        let initial = ResourceLogSearchResult.make(text: text, query: "")
        let searched = ResourceLogSearchResult.makeForInspector(
            text: text,
            textIndex: initial.textIndex,
            query: "error"
        )

        XCTAssertEqual(searched.textIndex, initial.textIndex)
        XCTAssertEqual(searched.matchRanges.count, 1)
    }

    @MainActor
    func testLogSearchKeepsNativeFieldEditorAndFrameAcrossEveryQueryResultAndThemeUpdate() async throws {
        let model = ResourceLogSearchFocusModel()
        let host = NSHostingController(
            rootView: ResourceLogSearchFocusHarness(model: model)
                .frame(width: 520, height: 100, alignment: .topLeading)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 520, height: 100),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { closeTestWindow(window) }

        try await settle(window: window)

        let searchField = try XCTUnwrap(
            findEditableTextFields(in: host.view).first,
            "Expected the native editable search field."
        )
        window.makeFirstResponder(searchField)
        searchField.selectText(nil)
        let originalFieldEditor = try XCTUnwrap(searchField.currentEditor() as? NSTextView)
        originalFieldEditor.setSelectedRange(NSRange(location: 0, length: 0))
        let originalFrame = host.view.convert(searchField.bounds, from: searchField)
        XCTAssertTrue(window.firstResponder === originalFieldEditor)

        var expectedQuery = ""
        for fragment in ["i", "n", "f", "o"] {
            originalFieldEditor.insertText(
                fragment,
                replacementRange: NSRange(location: originalFieldEditor.string.utf16.count, length: 0)
            )
            expectedQuery += fragment
            try await settle(window: window)
            XCTAssertEqual(model.query, expectedQuery)
            try assertStableLogSearchInput(
                in: host.view,
                window: window,
                expectedField: searchField,
                expectedEditor: originalFieldEditor,
                expectedFrame: originalFrame
            )

            model.searchSummary = ResourceLogSearchResult.make(
                text: model.text,
                query: expectedQuery
            )
            try await settle(window: window)
            try assertStableLogSearchInput(
                in: host.view,
                window: window,
                expectedField: searchField,
                expectedEditor: originalFieldEditor,
                expectedFrame: originalFrame
            )
        }

        for appearance in RuneAppearanceTheme.allCases {
            model.appearance = appearance
            try await settle(window: window)
            try assertStableLogSearchInput(
                in: host.view,
                window: window,
                expectedField: searchField,
                expectedEditor: originalFieldEditor,
                expectedFrame: originalFrame
            )
        }

        XCTAssertEqual(originalFieldEditor.selectedRange().location, expectedQuery.utf16.count)
    }

    @MainActor
    func testRapidTypingThroughLogPanePublishesOnlyTheLatestSearchWithoutLosingFocus() async throws {
        let text = "info ready\nINFO retry\nnoise\ninformation only\n"
        let host = NSHostingController(
            rootView: PodLogsInspectorPane(
                selectedLogPreset: .constant(.recentLines),
                includePreviousLogs: .constant(false),
                selectedContainer: .constant(""),
                isTailModeEnabled: .constant(false),
                isStreamPaused: .constant(false),
                isLoadingLogs: false,
                isLoadingResources: false,
                errorMessage: nil,
                statusText: "Ready",
                containerOptions: ["app"],
                logText: text,
                readOnlyResetID: "rapid-search-pane",
                onReload: {},
                onSave: {},
                onSaveVisibleZip: { _ in },
                onSaveFullZip: {},
                onSaveAllPodsZip: {},
                onCopySelection: {},
                onCopyAll: {},
                onToggleStreamPause: {}
            )
            .frame(width: 760, height: 620)
            .runeAppearanceTheme(RuneAppearanceTheme.graphiteBlue.resolvedTheme)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 760, height: 620),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { closeTestWindow(window) }

        try await settle(window: window)
        let searchField = try XCTUnwrap(findEditableTextFields(in: host.view).first)
        window.makeFirstResponder(searchField)
        searchField.selectText(nil)
        let fieldEditor = try XCTUnwrap(searchField.currentEditor() as? NSTextView)
        fieldEditor.setSelectedRange(NSRange(location: 0, length: fieldEditor.string.utf16.count))
        let originalFrame = host.view.convert(searchField.bounds, from: searchField)

        for fragment in ["i", "n", "f", "o"] {
            fieldEditor.insertText(fragment, replacementRange: fieldEditor.selectedRange())
        }
        try await Task.sleep(nanoseconds: 160_000_000)
        try await settle(window: window)

        XCTAssertEqual(fieldEditor.string, "info")
        try assertStableLogSearchInput(
            in: host.view,
            window: window,
            expectedField: searchField,
            expectedEditor: fieldEditor,
            expectedFrame: originalFrame
        )
        XCTAssertEqual(fieldEditor.selectedRange().location, 4)
        let logTextView = try XCTUnwrap(findTextViews(in: host.view).first { $0.string == text })
        XCTAssertEqual(
            logTextView.selectedRange(),
            NSRange(location: 0, length: 4),
            "The completed result must navigate using only the final 'info' query, never an older prefix."
        )
    }

    func testLargeTextRapidSearchLaneRunsAtMostOneJobAndPublishesLatestOnly() async throws {
        let text = (0..<30_000)
            .map { index in
                index.isMultiple(of: 3)
                    ? "line-\(index) level=INFO"
                    : "line-\(index) level=debug"
            }
            .joined(separator: "\n")
        let tracker = ResourceLogsWorkLaneTracker()
        let lane = ResourceLogsLatestWorkLane<ResourceLogSearchResult>()

        let first = Task {
            try await lane.run(priority: .userInitiated) {
                tracker.begin(query: "i")
                defer { tracker.end() }
                while !Task.isCancelled {}
                throw CancellationError()
            }
        }
        guard tracker.waitUntilStarted(query: "i") else {
            first.cancel()
            await lane.cancel()
            return XCTFail("The first synthetic large-text request never started.")
        }

        let second = Task {
            try await lane.run(priority: .userInitiated) {
                tracker.begin(query: "in")
                defer { tracker.end() }
                while !Task.isCancelled {}
                throw CancellationError()
            }
        }
        guard tracker.waitUntilStarted(query: "in") else {
            first.cancel()
            second.cancel()
            await lane.cancel()
            return XCTFail("The replacement request never started after cancelling the first.")
        }

        let latest = Task {
            try await lane.run(priority: .userInitiated) {
                tracker.begin(query: "info")
                defer { tracker.end() }
                return try ResourceLogSearchResult.makeForInspector(
                    text: text,
                    query: "info",
                    cancellationCheck: { try Task.checkCancellation() }
                )
            }
        }

        let latestResult = try await latest.value
        XCTAssertEqual(latestResult.query, "info")
        XCTAssertEqual(latestResult.matchRanges.count, 10_000)
        XCTAssertEqual(tracker.startedQueries, ["i", "in", "info"])
        XCTAssertEqual(tracker.maximumActiveCount, 1)

        let firstResult = await first.result
        let secondResult = await second.result
        for result in [firstResult, secondResult] {
            switch result {
            case .success:
                XCTFail("A superseded search request must never publish a result.")
            case let .failure(error):
                XCTAssertTrue(error is CancellationError)
            }
        }
    }

    @MainActor
    func testPendingSearchNeverRemountsWideLogRenderer() async throws {
        let text = Self.makeMockWidePodLogTranscript(lineCount: 150)
        let renderResult = ResourceLogSearchResult.makeForInspector(text: text, query: "")
        XCTAssertGreaterThan(text.utf8.count, ResourceLogsDeferredRenderingPolicy.deferredOutputThreshold)
        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: renderResult))

        let model = ResourceLogRenderStabilityModel(text: text, renderResult: renderResult)
        let host = NSHostingController(
            rootView: ResourceLogRenderStabilityHarness(model: model)
                .frame(width: 760, height: 520)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 760, height: 520),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { closeTestWindow(window) }

        try await settle(window: window)
        let originalScrollView = try XCTUnwrap(
            findScrollViews(in: host.view).max {
                ($0.documentView?.bounds.height ?? 0) < ($1.documentView?.bounds.height ?? 0)
            }
        )
        let originalDocumentView = try XCTUnwrap(originalScrollView.documentView)

        for query in ["m", "mo", "moc", "mock"] {
            model.navigationResult = nil
            try await settle(window: window)
            XCTAssertTrue(
                try XCTUnwrap(findScrollViews(in: host.view).first { $0 === originalScrollView })
                    === originalScrollView
            )

            let completedResult = ResourceLogSearchResult.makeForInspector(
                text: text,
                textIndex: renderResult.textIndex,
                query: query
            )
            let scrollOriginBeforePublication = originalScrollView.contentView.bounds.origin
            model.renderResult = completedResult
            try await settle(window: window)
            let rendererAfterPublication = try XCTUnwrap(
                findScrollViews(in: host.view).first { $0 === originalScrollView },
                "Completed search publication must leave the virtualized scroll view mounted."
            )
            XCTAssertTrue(rendererAfterPublication === originalScrollView)
            XCTAssertTrue(originalScrollView.documentView === originalDocumentView)
            XCTAssertEqual(originalScrollView.contentView.bounds.origin.x, scrollOriginBeforePublication.x, accuracy: 0.5)
            XCTAssertEqual(originalScrollView.contentView.bounds.origin.y, scrollOriginBeforePublication.y, accuracy: 0.5)

            model.navigationResult = completedResult
            model.navigationSequence += 1
            try await settle(window: window)
            XCTAssertTrue(
                try XCTUnwrap(findScrollViews(in: host.view).first { $0 === originalScrollView })
                    === originalScrollView
            )
        }
    }

    @MainActor
    func testLogSearchNavigationCentersActiveMatchAndCanRefocusSameResult() async throws {
        let text = (0..<420)
            .map { index in
                if index == 30 || index == 360 {
                    return "line-\(index) level=error marker=needle"
                }
                return "line-\(index) level=info"
            }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "needle")
        let model = ResourceLogRenderStabilityModel(text: text, renderResult: result)
        model.navigationResult = result
        model.navigationSequence = 1
        let host = NSHostingController(
            rootView: ResourceLogRenderStabilityHarness(model: model)
                .frame(width: 720, height: 360)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 720, height: 360),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { closeTestWindow(window) }

        try await settle(window: window)
        let textView = try XCTUnwrap(findTextViews(in: host.view).first { $0.string == text })
        let scrollView = try XCTUnwrap(textView.enclosingScrollView)

        model.selectedMatchIndex = 1
        model.navigationSequence &+= 1
        try await settle(window: window)

        XCTAssertEqual(textView.selectedRange(), result.matchRanges[1])
        assertRangeIsCentered(result.matchRanges[1], in: textView, scrollView: scrollView)

        scrollView.contentView.scroll(to: .zero)
        scrollView.reflectScrolledClipView(scrollView.contentView)
        XCTAssertLessThan(scrollView.contentView.bounds.midY, textView.bounds.midY)

        model.navigationSequence &+= 1
        try await settle(window: window)

        XCTAssertEqual(textView.selectedRange(), result.matchRanges[1])
        assertRangeIsCentered(result.matchRanges[1], in: textView, scrollView: scrollView)

        scrollView.contentView.scroll(to: .zero)
        scrollView.reflectScrolledClipView(scrollView.contentView)
        let appendedText = text + "\nline-421 level=info"
        let appendedResult = ResourceLogSearchResult.makeForInspector(
            text: appendedText,
            query: "needle"
        )
        model.text = appendedText
        model.renderResult = appendedResult
        model.navigationResult = appendedResult
        try await settle(window: window)

        XCTAssertEqual(
            scrollView.contentView.bounds.origin.y,
            0,
            accuracy: 1,
            "Publishing append-only search ranges without a new navigation sequence must preserve manual scroll."
        )
    }

    @MainActor
    func testStreamedWideLogAppendKeepsVirtualizedRendererPolicyBeforeAndAfterIndexing() async throws {
        let initialText = Self.makeMockWidePodLogTranscript(lineCount: 149)
        let appendedText = Self.makeMockWidePodLogTranscript(lineCount: 150)
        let initialResult = ResourceLogSearchResult.makeForInspector(text: initialText, query: "")
        let appendedResult = ResourceLogSearchResult.makeForInspector(text: appendedText, query: "")
        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: initialText))
        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: appendedText))
        XCTAssertEqual(
            ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: appendedText),
            ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: appendedResult)
        )

        let model = ResourceLogRenderStabilityModel(text: initialText, renderResult: initialResult)
        let host = NSHostingController(
            rootView: ResourceLogRenderStabilityHarness(model: model)
                .frame(width: 760, height: 520)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 760, height: 520),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { closeTestWindow(window) }

        try await settle(window: window)
        XCTAssertTrue(findTextViews(in: host.view).allSatisfy { $0.string != initialText })
        XCTAssertFalse(findScrollViews(in: host.view).isEmpty)

        model.text = appendedText
        model.renderResult = nil
        try await settle(window: window)
        XCTAssertTrue(findTextViews(in: host.view).allSatisfy { $0.string != appendedText })

        model.renderResult = appendedResult
        try await settle(window: window)
        XCTAssertTrue(findTextViews(in: host.view).allSatisfy { $0.string != appendedText })
        XCTAssertFalse(findScrollViews(in: host.view).isEmpty)
    }

    func testLogInspectorShowsInterruptedStreamStateInsideOutputSurface() throws {
        let logsViewSource = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(logsViewSource.contains("ResourceLogsErrorView(message: errorMessage, onReload: onReload)"))
        XCTAssertTrue(logsViewSource.contains("RuneContentStateAction("))
        XCTAssertTrue(logsViewSource.contains("\"Retry\","))
        XCTAssertTrue(logsViewSource.contains("perform: onReload"))
        XCTAssertTrue(logsViewSource.contains("Label(t(.reload), systemImage: \"arrow.clockwise\")"))
        XCTAssertTrue(rootViewSource.contains("errorMessage: viewModel.state.lastLogFetchError"))
        XCTAssertTrue(rootViewSource.contains("return \"Reconnect failed\""))
    }

    func testLogStatusUsesCompactChipInsteadOfFullWidthBanner() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        let statusPanelSource = try XCTUnwrap(source.slice(
            from: "private struct LogToolbarStatusIndicator",
            to: "private struct LogToolbarSourceSummary"
        ))

        XCTAssertTrue(statusPanelSource.contains(".frame(width: 20, height: RuneUILayoutMetrics.inspectorToolbarControlMinHeight)"))
        XCTAssertTrue(statusPanelSource.contains("statusColor.opacity(0.18)"))
        XCTAssertTrue(statusPanelSource.contains(".help(helpText)"))
        XCTAssertTrue(statusPanelSource.contains(".accessibilityLabel(\"Log status: \\(statusText)\")"))
        XCTAssertFalse(statusPanelSource.contains("Text(compactText)"))
        XCTAssertFalse(statusPanelSource.contains("private var compactText"))
        XCTAssertFalse(statusPanelSource.contains(".frame(maxWidth: .infinity"))
        XCTAssertFalse(statusPanelSource.contains(".strokeBorder(statusColor.opacity(0.22)"))
    }

    func testPodLogToolbarUsesEmbeddedFavoritePicker() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let sourceControls = try XCTUnwrap(source.slice(
            from: "private var sourceControls: some View",
            to: "private var primaryControls: some View"
        ))

        XCTAssertTrue(sourceControls.contains("FavoritePodPicker("))
        XCTAssertTrue(sourceControls.contains("width: ResourceLogsLayoutMetrics.podPickerWidth"))
        XCTAssertTrue(sourceControls.contains("rowTitle: { $0.name }"))
        XCTAssertTrue(sourceControls.contains("rowDetail: { \"\\($0.namespace) - \\($0.status)\" }"))
        XCTAssertTrue(sourceControls.contains("isFavoritePod: isFavoritePod"))
        XCTAssertTrue(sourceControls.contains("onToggleFavoritePod: onToggleFavoritePod"))
        XCTAssertTrue(sourceControls.contains("selection: selectedPodID"))
        XCTAssertTrue(sourceControls.contains(".accessibilityIdentifier(\"pod-log-favorite-picker\")"))
        XCTAssertFalse(sourceControls.contains("podPickerTitle(pod)"))
        XCTAssertFalse(sourceControls.contains("pod-log-favorite-toggle"))
    }

    func testFixedLogSourceControlsExposeFullValuesOnHover() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let summarySource = try XCTUnwrap(source.slice(
            from: "private struct LogToolbarSourceSummary",
            to: "struct ResourceStructuredLogSummaryPanel"
        ))

        XCTAssertTrue(source.contains("popup.toolTip = text"))
        XCTAssertTrue(source.contains("popup.setAccessibilityHelp(text)"))
        XCTAssertTrue(summarySource.contains("Label(joinedValues"))
        XCTAssertTrue(summarySource.contains(".help(\"\\(title): \\(joinedValues)\")"))
        XCTAssertTrue(summarySource.contains("values.joined(separator: \", \")"))
    }

    func testFavoritePodPickerSelectionSurvivesFavoriteSorting() {
        let pods = [
            PodSummary(name: "api", namespace: "default", status: "Running"),
            PodSummary(name: "worker", namespace: "default", status: "Running"),
            PodSummary(name: "queue", namespace: "default", status: "Running")
        ]
        var favorites = Set([pods[1].id])

        XCTAssertEqual(
            FavoritePodPickerPresentation.sortedPods(pods, isFavoritePod: { favorites.contains($0.id) }).map(\.id),
            [pods[1].id, pods[0].id, pods[2].id]
        )
        XCTAssertEqual(
            FavoritePodPickerPresentation.selectedPod(in: pods, selection: pods[0].id)?.id,
            pods[0].id
        )

        favorites.insert(pods[0].id)

        XCTAssertEqual(
            FavoritePodPickerPresentation.sortedPods(pods, isFavoritePod: { favorites.contains($0.id) }).map(\.id),
            [pods[0].id, pods[1].id, pods[2].id]
        )
        XCTAssertEqual(
            FavoritePodPickerPresentation.selectedPod(in: pods, selection: pods[2].id)?.id,
            pods[2].id,
            "Changing favorites must not pin the picker to the favorite pod; selection remains an independent pod id."
        )
    }

    func testTailControlDoesNotAddPauseButtonBesideTail() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let toolbarActions = try XCTUnwrap(source.slice(
            from: "private var toolbarActions: some View",
            to: "private var language: RuneLanguage"
        ))

        XCTAssertTrue(toolbarActions.contains("tailControl"))
        XCTAssertTrue(toolbarActions.contains("LogToolbarStatusIndicator("))
        XCTAssertTrue(toolbarActions.contains("statusText: statusText"))
        XCTAssertTrue(toolbarActions.contains("showsPreviousHint: includePreviousLogs"))
        XCTAssertTrue(toolbarActions.contains("toolbarIconLabel(t(.previous), systemImage: \"clock.arrow.circlepath\""))
        XCTAssertTrue(source.contains("if isStreamPaused"))
        XCTAssertTrue(source.contains("toolbarIconLabel(t(.resume), systemImage: \"play.fill\""))
        XCTAssertTrue(source.contains("toolbarIconLabel(t(.pause), systemImage: \"pause.fill\""))
        XCTAssertTrue(source.contains(".buttonStyle(.bordered)"))
        XCTAssertTrue(source.contains(".buttonStyle(.borderedProminent)"))
        XCTAssertTrue(source.contains(".logToolbarIconButtonFrame()"))
        XCTAssertTrue(source.contains("Button(t(.stopTail))"))
        XCTAssertFalse(source.contains("Toggle(\"Tail\""))
        XCTAssertFalse(source.contains("Button(isStreamPaused ? \"Resume\" : \"Pause\""))
    }

    func testTerminalCompactLogsToolbarKeepsSourcesOnFirstRowAndConsolidatedActionsOnSecondRow() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let compactBody = try XCTUnwrap(source.slice(
            from: "private var terminalCompactBody: some View",
            to: "private var sourceControls: some View"
        ))

        XCTAssertTrue(compactBody.contains("LogToolbarScrollRow {\n                primaryControls"))
        XCTAssertTrue(compactBody.contains("LogToolbarScrollRow {\n                toolbarActions"))
        XCTAssertFalse(compactBody.contains("LogToolbarScrollRow {\n                modeControls"))
        XCTAssertFalse(source.contains("private var modeControls: some View"))
        XCTAssertFalse(compactBody.contains("ResourceLogsStatusPanel("))
        XCTAssertFalse(compactBody.contains("statusText: statusText,\n                showsPreviousHint: includePreviousLogs"))
    }

    func testLogToolbarStatusIndicatorLivesWithActionControlsAndHasHelp() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let indicatorSource = try XCTUnwrap(source.slice(
            from: "private struct LogToolbarStatusIndicator",
            to: "private struct LogToolbarSourceSummary"
        ))

        XCTAssertTrue(indicatorSource.contains(".help(helpText)"))
        XCTAssertTrue(indicatorSource.contains(".accessibilityLabel(\"Log status: \\(statusText)\")"))
        XCTAssertTrue(indicatorSource.contains("Previous logs only exist for restarted containers."))
        XCTAssertTrue(indicatorSource.contains("RuneUILayoutMetrics.inspectorToolbarControlMinHeight"))
        XCTAssertFalse(indicatorSource.contains("Text(compactText)"))
    }

    func testLogToolbarIconOnlyPreviousAndTailControlsExposeHoverHelp() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let toolbarSource = try XCTUnwrap(source.slice(
            from: "struct ResourceLogsToolbar: View",
            to: "private extension View"
        ))

        XCTAssertTrue(toolbarSource.contains("toolbarIconLabel(t(.previous), systemImage: \"clock.arrow.circlepath\", help: t(.previousLogsHelp))"))
        XCTAssertTrue(toolbarSource.contains("toolbarIconLabel(t(.tail), systemImage: \"play.fill\", help: t(.startTailHelp))"))
        XCTAssertTrue(toolbarSource.contains("toolbarIconLabel(t(.pause), systemImage: \"pause.fill\", help: t(.pauseTailHelp))"))
        XCTAssertTrue(toolbarSource.contains("toolbarIconLabel(t(.resume), systemImage: \"play.fill\", help: t(.resumeTailHelp))"))
        XCTAssertTrue(toolbarSource.contains("Label(title, systemImage: systemImage)"))
        XCTAssertTrue(toolbarSource.contains(".help(help)"))
        XCTAssertTrue(toolbarSource.contains(".accessibilityLabel(title)"))
    }

    func testTerminalLogTabsExposePodNamesOnHover() throws {
        let tabBarSource = try String(contentsOfFile: terminalLogTabBarPath, encoding: .utf8)

        XCTAssertTrue(tabBarSource.contains("Text(\"\\(number) \\(tab.title)\")"))
        XCTAssertTrue(tabBarSource.contains(".help(tab.helpText)"))
        XCTAssertTrue(tabBarSource.contains("Text(subtitle)"))
        XCTAssertTrue(tabBarSource.contains("\"Close log tab for \\(tab.title)\""))
        XCTAssertTrue(tabBarSource.contains("RuneIconButton("))
    }

    func testTerminalLogTabsUseRuneSurfaceChrome() throws {
        let tabBarSource = try String(contentsOfFile: terminalLogTabBarPath, encoding: .utf8)
        let tabStripSource = try String(contentsOfFile: terminalTabStripPath, encoding: .utf8)
        let logsInspectorSource = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(tabBarSource.contains("TerminalTabStrip("))
        XCTAssertTrue(tabBarSource.contains(".terminalTabChrome(isActive: tab.id == activeTabID)"))
        XCTAssertTrue(tabStripSource.contains("RuneSurfaceBackground(kind: .inset)"))
        XCTAssertTrue(tabStripSource.contains("RuneSurfaceBackground(kind: .listRow(isSelected: isActive))"))
        XCTAssertTrue(tabStripSource.contains("Capsule()"))
        XCTAssertTrue(tabStripSource.contains(".frame(width: 3, height: 16)"))
        XCTAssertFalse(tabBarSource.contains("RuneSurfaceBackground(kind: .editor)"))
        XCTAssertFalse(tabBarSource.contains(".frame(height: 2)"))
        XCTAssertFalse(logsInspectorSource.contains("struct TerminalLogTabBar: View"))
    }

    func testLogSearchReturnsOriginalTextWhenQueryIsBlank() {
        let result = ResourceLogSearchResult.make(
            text: "alpha\nbeta\ngamma",
            query: "   "
        )

        XCTAssertEqual(result.displayedText, "alpha\nbeta\ngamma")
        XCTAssertFalse(result.isFiltering)
    }

    func testBlankLogSearchCountsCommonLineEndings() {
        let result = ResourceLogSearchResult.make(
            text: "alpha\r\nbeta\rgamma\n",
            query: ""
        )

        XCTAssertEqual(result.totalLineCount, 4)
        XCTAssertEqual(result.matchingLineCount, 4)
    }

    func testLogSearchFindsMatchesWithoutFilteringOutput() {
        let result = ResourceLogSearchResult.make(
            text: "INFO started\nwarn slow query\nERROR failed\nsecond error\n",
            query: "error"
        )

        XCTAssertEqual(result.matchingLineCount, 2)
        XCTAssertEqual(result.displayedText, "INFO started\nwarn slow query\nERROR failed\nsecond error\n")
        XCTAssertEqual(result.summaryText, "2 matches in 5 total lines.")
        XCTAssertEqual(result.matchRanges.count, 2)
        XCTAssertEqual(result.matchLineNumbers, [3, 4])
        XCTAssertEqual(result.matchPositionText(selectedIndex: 0), "1 of 2")
        XCTAssertEqual(result.matchPositionText(selectedIndex: 1), "2 of 2")
        XCTAssertEqual(result.matchPositionText(selectedIndex: 99), "2 of 2")
        XCTAssertEqual((result.displayedText as NSString).substring(with: result.matchRanges[0]), "ERROR")
        XCTAssertEqual((result.displayedText as NSString).substring(with: result.matchRanges[1]), "error")
    }

    func testLogSearchNavigationWrapsAndBuildsRangeRequest() {
        let result = ResourceLogSearchResult.make(
            text: "INFO started\nERROR failed\nsecond error\n",
            query: "error"
        )

        XCTAssertEqual(result.nextMatchIndex(from: 0), 1)
        XCTAssertEqual(result.nextMatchIndex(from: 1), 0)
        XCTAssertEqual(result.previousMatchIndex(from: 0), 1)

        let request = result.navigationRequest(selectedIndex: 1, sequence: 3)
        XCTAssertEqual(request?.sequence, 3)
        XCTAssertEqual(request?.range?.location, result.matchRanges[1].location)
        XCTAssertEqual(request?.range?.length, result.matchRanges[1].length)
    }

    func testStructuredFieldSearchQueryMatchesRawJSONWithoutFilteringOutput() {
        let text = """
        {"level":"info","message":"started","pod":"api-0"}
        {"level":"error","message":"failed","pod":"api-1"}
        {"level":"info","message":"recovered","pod":"api-1"}
        """
        let query = ResourceStructuredLogFieldSearch.query(fieldKey: "level", value: "error")
        let result = ResourceLogSearchResult.make(text: text, query: query)

        XCTAssertEqual(query, #""level":"error""#)
        XCTAssertEqual(result.matchingLineCount, 1)
        XCTAssertEqual(result.displayedText, text)
        XCTAssertTrue(result.displayedText.contains(#""level":"info""#))
        XCTAssertTrue(result.displayedText.contains(#""level":"error""#))
    }

    func testStructuredFieldAliasSearchQueryMatchesRawJSONWithoutFilteringOutput() throws {
        let text = """
        {"severity":"error","msg":"failed","requestId":"req-1"}
        {"level":"info","message":"recovered","request_id":"req-2"}
        """
        let summary = ResourceStructuredLogAnalyzer.analyze(text: text)
        let field = try XCTUnwrap(summary.field("level"))
        let query = ResourceStructuredLogFieldSearch.query(field: field, value: "error")
        let result = ResourceLogSearchResult.make(text: text, query: query)

        XCTAssertEqual(query, #""severity":"error""#)
        XCTAssertEqual(result.matchingLineCount, 1)
        XCTAssertEqual(result.displayedText, text)
        XCTAssertTrue(result.displayedText.contains(#""level":"info""#))
        XCTAssertTrue(result.displayedText.contains(#""severity":"error""#))
    }

    func testStructuredLogSummaryPanelIsWiredIntoLogInspector() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("ResourceStructuredLogAnalyzer.analyze(text:"))
        XCTAssertTrue(source.contains("ResourceStructuredLogSummaryPanel("))
        XCTAssertTrue(source.contains("structuredSummary: structuredLogSummary"))
        XCTAssertTrue(source.contains("ResourceStructuredLogFieldSearch.query(field: field, value: value)"))
        XCTAssertTrue(source.contains("onSearchFieldSample"))
        XCTAssertTrue(source.contains("onSearchDuplicate"))
    }

    func testSimpleModeSkipsStructuredLogSummaryAnalysis() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("@AppStorage(RuneSettingsKeys.simpleMode) private var simpleMode = false"))
        XCTAssertTrue(source.contains("showsInsights: !simpleMode"))
        XCTAssertTrue(source.contains(".onChange(of: simpleMode)"))
        XCTAssertTrue(source.contains("scheduleStructuredSummary(for: logText, debounced: false)"))
        XCTAssertTrue(source.contains("guard !simpleMode else"))
        XCTAssertTrue(source.contains("structuredLogSummary = ResourceStructuredLogAnalyzer.analyze(text: \"\")"))
    }

    func testLogSearchUIShowsCurrentMatchPosition() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("matchPositionText(selectedIndex: selectedMatchIndex)"))
        XCTAssertTrue(source.contains("Text(matchStatusText)"))
        XCTAssertTrue(source.contains("guard searchSummary.hasMatches else { return \"No results\" }"))
        XCTAssertFalse(source.contains("ResourceLogsSearchSummaryBar("))
        XCTAssertTrue(source.contains("selectedSearchMatchIndex: $selectedSearchMatchIndex"))
        XCTAssertTrue(source.contains(".monospacedDigit()"))
        XCTAssertTrue(source.contains("@State private var isJumpPopoverPresented = false"))
        XCTAssertTrue(source.contains("prepareJumpPopover(for: searchSummary)"))
        XCTAssertTrue(source.contains("jumpToMatchPopover(matchCount: jumpMatchCount)"))
        XCTAssertTrue(source.contains("private func commitJump(matchCount: Int)"))
        XCTAssertTrue(source.contains("Button(\"Go\")"))
        XCTAssertTrue(source.contains("Go to match"))
        XCTAssertTrue(source.contains(".background(RuneSurfaceBackground(kind: .editor))"))
    }

    func testLogScrollIdentityIgnoresTailContentChanges() {
        let first = ResourceLogSearchResult.make(
            text: "INFO started",
            query: ""
        )
        let second = ResourceLogSearchResult.make(
            text: "INFO started\nINFO ready",
            query: ""
        )

        XCTAssertEqual(first.scrollIdentityToken, second.scrollIdentityToken)
    }

    func testLogScrollIdentityDoesNotResetWhenSearchQueryChanges() {
        let first = ResourceLogSearchResult.make(
            text: "INFO started\nERROR failed",
            query: ""
        )
        let second = ResourceLogSearchResult.make(
            text: "INFO started\nERROR failed",
            query: "error"
        )

        XCTAssertEqual(first.scrollIdentityToken, second.scrollIdentityToken)
    }

    func testLargeUnfilteredLogsUseVirtualizedTextSurface() {
        let text = String(repeating: "INFO synthetic benchmark line\n", count: 12_000)
        let result = ResourceLogSearchResult.make(text: text, query: "")

        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))
    }

    func testSmallUnfilteredLogsRenderImmediately() {
        let result = ResourceLogSearchResult.make(text: "INFO ready\nINFO steady", query: "")

        XCTAssertFalse(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))
    }

    func testPendingRendererPolicyMatchesIndexedPolicyAcrossCommonLineEndings() {
        let payload = String(repeating: "x", count: 280)
        for separator in ["\n", "\r\n", "\r"] {
            for lineCount in [999, 1_000] {
                let text = (0..<lineCount)
                    .map { "synthetic-\($0)-\(payload)" }
                    .joined(separator: separator)
                let indexed = ResourceLogSearchResult.makeForInspector(text: text, query: "")

                XCTAssertGreaterThan(text.utf8.count, ResourceLogsDeferredRenderingPolicy.deferredOutputThreshold)
                XCTAssertEqual(indexed.textIndex.lineCount, lineCount)
                XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: text))
                XCTAssertEqual(
                    ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: text),
                    ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: indexed),
                    "Pending and indexed renderer selection must agree for \(lineCount) lines."
                )
            }
        }
    }

    func testLargeSearchedLogsUseVirtualizedTextSurfaceWithoutCappingData() {
        let text = (0..<12_000)
            .map { index in
                index.isMultiple(of: 1_000)
                    ? "ERROR synthetic line \(index)"
                    : "INFO synthetic line \(index)"
            }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.make(text: text, query: "error")

        XCTAssertTrue(result.isFiltering)
        XCTAssertTrue(result.displayedText.contains("INFO synthetic line 1"))
        XCTAssertTrue(result.displayedText.contains("INFO synthetic line 11999"))
        XCTAssertEqual(result.matchRanges.count, 12)
        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))
    }

    func testWideFewLineLogsUseVirtualizedTextSurface() throws {
        let widePayload = String(repeating: " payload=synthetic-wide-log-field", count: 140)
        let text = (0..<80)
            .map { index in "line-\(String(format: "%06d", index))\(widePayload)" }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "")

        XCTAssertGreaterThan(result.displayedText.utf8.count, ResourceLogsDeferredRenderingPolicy.deferredOutputThreshold)
        XCTAssertEqual(result.textIndex.lineCount, 80)
        XCTAssertTrue(
            ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result),
            "Payload size alone should move very wide logs off the expensive AppKit renderer."
        )

        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        XCTAssertTrue(source.contains("allowsAutomaticLargeTextSurface: false"))
    }

    func testLargeInspectorSearchDoesNotCapRenderedLogData() {
        let text = (0..<12_000)
            .map { index in
                index.isMultiple(of: 1_500)
                    ? "ERROR full output line \(index)"
                    : "INFO full output line \(index)"
            }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "error")

        XCTAssertEqual(result.displayedText, text)
        XCTAssertTrue(result.displayedText.contains("INFO full output line 11999"))
        XCTAssertEqual(result.matchRanges.count, 8)
        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))
    }

    func testLargeLogSearchNavigationKeepsFocusWhenNextMatchStaysOnSameLine() throws {
        let text = "ERROR first ERROR second\nINFO recovered\n"
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "error")

        XCTAssertEqual(result.matchLineNumber(selectedIndex: 0), 1)
        XCTAssertEqual(result.matchLineNumber(selectedIndex: 1), 1)
        XCTAssertNotEqual(
            result.largeTextNavigationRevision(selectedIndex: 0, sequence: 1),
            result.largeTextNavigationRevision(selectedIndex: 1, sequence: 1)
        )

        let inspectorSource = try String(contentsOfFile: inspectorTextViewsPath, encoding: .utf8)
        let logsSource = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let sharedSource = try String(contentsOfFile: largeTextSurfacePath, encoding: .utf8)

        XCTAssertTrue(inspectorSource.contains("largeTextScrollTargetRevision"))
        XCTAssertTrue(logsSource.contains("largeTextNavigationRevision"))
        XCTAssertTrue(sharedSource.contains("scrollTargetRevision"))
        XCTAssertTrue(
            sharedSource.contains("matchScrollTargetID"),
            "Virtualized log search must use a per-navigation scroll identity so “Next match” still scrolls when the line number is unchanged."
        )
        XCTAssertTrue(sharedSource.contains("searchMatchRanges"))
        XCTAssertTrue(sharedSource.contains("highlightedText"))
        XCTAssertTrue(sharedSource.contains("findHighlightColor"))
        XCTAssertTrue(sharedSource.contains("easeOut(duration: 0.12)"))
    }

    @MainActor
    func testLargeLogSearchNavigationCentersDeepMatch() async throws {
        let lineCount = 1_400
        let payload = String(repeating: " synthetic-payload", count: 14)
        let text = (0..<lineCount)
            .map { index in
                let marker = index == 20 || index == 1_200 ? " marker=needle" : ""
                return "line-\(index)\(marker)\(payload)"
            }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "needle")
        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))
        XCTAssertEqual(result.matchLineNumbers, [21, 1_201])

        let model = ResourceLogRenderStabilityModel(text: text, renderResult: result)
        model.navigationResult = result
        model.navigationSequence = 1
        let host = NSHostingController(
            rootView: ResourceLogRenderStabilityHarness(model: model)
                .frame(width: 760, height: 420)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 760, height: 420),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { closeTestWindow(window) }

        try await settle(window: window)
        model.selectedMatchIndex = 1
        model.navigationSequence &+= 1
        try await Task.sleep(nanoseconds: 400_000_000)
        try await settle(window: window)

        let scrollView = try XCTUnwrap(
            findScrollViews(in: host.view).max {
                ($0.documentView?.bounds.height ?? 0) < ($1.documentView?.bounds.height ?? 0)
            }
        )
        let documentHeight = try XCTUnwrap(scrollView.documentView?.bounds.height)
        let estimatedRowHeight = documentHeight / CGFloat(lineCount)
        let targetMidY = CGFloat(1_200) * estimatedRowHeight + estimatedRowHeight / 2
        let visibleMidY = scrollView.contentView.bounds.midY

        XCTAssertGreaterThan(scrollView.contentView.bounds.minY, documentHeight * 0.6)
        XCTAssertLessThan(
            (targetMidY - visibleMidY).magnitude,
            max(estimatedRowHeight * 5, scrollView.contentView.bounds.height * 0.22),
            "The virtualized renderer should place a deep active match near the viewport center."
        )
    }

    /// Regression guard for pod logs made up of relatively few very wide rows: the virtualized surface must size
    /// the full vertical transcript instead of leaving a blank panel below its initial render window.
    @MainActor
    func testWideSyntheticPodLogsLayOutTranscriptTallerThanEightyLineStride() async throws {
        let lineCount = 150
        let text = Self.makeMockWidePodLogTranscript(lineCount: lineCount)
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "")

        XCTAssertGreaterThan(text.utf8.count, ResourceLogsDeferredRenderingPolicy.deferredOutputThreshold)
        XCTAssertEqual(result.textIndex.lineCount, lineCount)
        XCTAssertTrue(
            ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result),
            "Very wide payloads should use the virtualized renderer even below 1,000 lines."
        )

        let host = NSHostingController(
            rootView: PodLogsInspectorPane(
                selectedLogPreset: .constant(.largeTail),
                includePreviousLogs: .constant(false),
                selectedContainer: .constant(""),
                isTailModeEnabled: .constant(false),
                isStreamPaused: .constant(false),
                isLoadingLogs: false,
                isLoadingResources: false,
                errorMessage: nil,
                statusText: "Tail off",
                containerOptions: ["mock"],
                logText: text,
                readOnlyResetID: "mock-wide-pod-log-layout",
                onReload: {},
                onSave: {},
                onSaveVisibleZip: { _ in },
                onSaveFullZip: {},
                onSaveAllPodsZip: {},
                onCopySelection: {},
                onCopyAll: {},
                onToggleStreamPause: {}
            )
            .frame(width: 920, height: 680)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 920, height: 680),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { closeTestWindow(window) }

        try await settle(window: window)

        let documentHeight = try XCTUnwrap(
            findScrollViews(in: host.view).compactMap { $0.documentView?.bounds.height }.max(),
            "Expected a virtualized log scroll view."
        )

        XCTAssertGreaterThan(
            documentHeight,
            CGFloat(lineCount) * 14,
            "Mock transcript has \(lineCount) logical lines; the virtual document must remain taller than an ~80-line render window."
        )
    }

    /// Many lines plus large UTF-8 use the virtualized log surface; scrollable content must still span the full
    /// line count (not an ~80-line-tall blank cut-off).
    @MainActor
    func testDeepSyntheticPodLogsScrollExceedsEightyLineStride() async throws {
        let lineCount = 1_100
        let text = Self.makeMockDeepPodLogTranscript(lineCount: lineCount, minUTF8PerLine: 240)
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "")

        XCTAssertGreaterThan(text.utf8.count, ResourceLogsDeferredRenderingPolicy.deferredOutputThreshold)
        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))

        let host = NSHostingController(
            rootView: PodLogsInspectorPane(
                selectedLogPreset: .constant(.largeTail),
                includePreviousLogs: .constant(false),
                selectedContainer: .constant(""),
                isTailModeEnabled: .constant(false),
                isStreamPaused: .constant(false),
                isLoadingLogs: false,
                isLoadingResources: false,
                errorMessage: nil,
                statusText: "Tail off",
                containerOptions: ["mock"],
                logText: text,
                readOnlyResetID: "mock-deep-pod-log-layout",
                onReload: {},
                onSave: {},
                onSaveVisibleZip: { _ in },
                onSaveFullZip: {},
                onSaveAllPodsZip: {},
                onCopySelection: {},
                onCopyAll: {},
                onToggleStreamPause: {}
            )
            .frame(width: 920, height: 680)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 920, height: 680),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { closeTestWindow(window) }

        try await settle(window: window)

        let scrollViews = findScrollViews(in: host.view)
        let docHeights = scrollViews.compactMap { $0.documentView?.bounds.height }
        let maxDocHeight = try XCTUnwrap(docHeights.max(), "Expected at least one scrollable document surface.")
        let expectedMinHeight = CGFloat(lineCount) * 14

        XCTAssertGreaterThan(
            maxDocHeight,
            expectedMinHeight,
            "Deep mock logs with \(lineCount) lines must size scroll content near one row per logical line; an ~80-row-tall document with thousands of lines is the reported pod log clip-off bug."
        )
    }

    @MainActor
    func testWideFewLinePodLogsFillInspectorOutputHeight() async throws {
        let longPayload = String(repeating: " payload=synthetic-wide-log-field", count: 140)
        let text = (0..<80)
            .map { index in "line-\(String(format: "%06d", index))\(longPayload)" }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "")

        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))
        XCTAssertEqual(result.textIndex.lineCount, 80)

        let host = NSHostingController(
            rootView: PodLogsInspectorPane(
                selectedLogPreset: .constant(.largeTail),
                includePreviousLogs: .constant(false),
                selectedContainer: .constant(""),
                isTailModeEnabled: .constant(false),
                isStreamPaused: .constant(false),
                isLoadingLogs: false,
                isLoadingResources: false,
                errorMessage: nil,
                statusText: "Tail off",
                containerOptions: ["app"],
                logText: text,
                readOnlyResetID: "large-few-line-log-layout",
                onReload: {},
                onSave: {},
                onSaveVisibleZip: { _ in },
                onSaveFullZip: {},
                onSaveAllPodsZip: {},
                onCopySelection: {},
                onCopyAll: {},
                onToggleStreamPause: {}
            )
            .frame(width: 920, height: 680)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 920, height: 680),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { closeTestWindow(window) }

        try await settle(window: window)

        let scrollViews = findScrollViews(in: host.view)
        let outputScrollView = try XCTUnwrap(
            scrollViews.max(by: { $0.frame.height < $1.frame.height }),
            "The large log output should mount a scroll view."
        )

        XCTAssertGreaterThanOrEqual(
            outputScrollView.frame.height,
            360,
            "Large few-line logs must use the remaining inspector height; otherwise only a few lines render and the rest of the panel looks blank."
        )
    }

    private var resourceLogsInspectorViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceLogsInspectorView.swift").path
    }

    private var terminalLogTabBarPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalLogTabBar.swift").path
    }

    private var terminalTabStripPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalTabStrip.swift").path
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

    private var runeFindBarControlsPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Layout/RuneFindBarControls.swift").path
    }

    private var largeTextSurfacePath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneSharedUI/RuneSharedComponents.swift").path
    }

    @MainActor
    private func settle(window: NSWindow) async throws {
        for _ in 0..<8 {
            window.contentView?.layoutSubtreeIfNeeded()
            try await Task.sleep(nanoseconds: 25_000_000)
        }
    }

    @MainActor
    private func closeTestWindow(_ window: NSWindow) {
        window.orderOut(nil)
        window.contentViewController = nil
        window.contentView = nil
    }

    @MainActor
    private func assertRangeIsCentered(
        _ range: NSRange,
        in textView: NSTextView,
        scrollView: NSScrollView,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        guard let layoutManager = textView.layoutManager,
              let textContainer = textView.textContainer else {
            return XCTFail("Missing text layout infrastructure.", file: file, line: line)
        }
        layoutManager.ensureLayout(forCharacterRange: range)
        let glyphRange = layoutManager.glyphRange(
            forCharacterRange: range,
            actualCharacterRange: nil
        )
        var targetRect = layoutManager.boundingRect(
            forGlyphRange: glyphRange,
            in: textContainer
        )
        targetRect.origin.x += textView.textContainerOrigin.x
        targetRect.origin.y += textView.textContainerOrigin.y
        let visibleBounds = scrollView.contentView.bounds

        XCTAssertLessThan(
            abs(targetRect.midY - visibleBounds.midY),
            visibleBounds.height * 0.28,
            "The active search match should settle near the viewport center.",
            file: file,
            line: line
        )
    }

    @MainActor
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

    @MainActor
    private func findTextViews(in view: NSView) -> [NSTextView] {
        var matches: [NSTextView] = []
        if let textView = view as? NSTextView {
            matches.append(textView)
        }
        for subview in view.subviews {
            matches.append(contentsOf: findTextViews(in: subview))
        }
        return matches
    }

    @MainActor
    private func findEditableTextFields(in view: NSView) -> [NSTextField] {
        var matches: [NSTextField] = []
        if let textField = view as? NSTextField, textField.isEditable {
            matches.append(textField)
        }
        for subview in view.subviews {
            matches.append(contentsOf: findEditableTextFields(in: subview))
        }
        return matches
    }

    @MainActor
    private func findPopUpButtons(in view: NSView) -> [NSPopUpButton] {
        var matches: [NSPopUpButton] = []
        if let popup = view as? NSPopUpButton {
            matches.append(popup)
        }
        for subview in view.subviews {
            matches.append(contentsOf: findPopUpButtons(in: subview))
        }
        return matches
    }

    @MainActor
    private func renderedPNG(from view: NSView) throws -> Data {
        view.layoutSubtreeIfNeeded()
        let bounds = view.bounds
        let representation = try XCTUnwrap(view.bitmapImageRepForCachingDisplay(in: bounds))
        view.cacheDisplay(in: bounds, to: representation)
        return try XCTUnwrap(representation.representation(using: .png, properties: [:]))
    }

    @MainActor
    private func assertStableLogSearchInput(
        in hostView: NSView,
        window: NSWindow,
        expectedField: NSTextField,
        expectedEditor: NSTextView,
        expectedFrame: NSRect,
        file: StaticString = #filePath,
        line: UInt = #line
    ) throws {
        let fields = findEditableTextFields(in: hostView)
        XCTAssertEqual(fields.count, 1, "Search updates must not create duplicate native fields.", file: file, line: line)
        let field = try XCTUnwrap(fields.first, file: file, line: line)
        XCTAssertTrue(field === expectedField, "Search updates must preserve the native field instance.", file: file, line: line)
        XCTAssertTrue(field.currentEditor() === expectedEditor, "Search updates must preserve the field editor.", file: file, line: line)
        XCTAssertTrue(window.firstResponder === expectedEditor, "Typing must retain first responder.", file: file, line: line)

        let frame = hostView.convert(field.bounds, from: field)
        XCTAssertEqual(frame.minX, expectedFrame.minX, accuracy: 0.5, file: file, line: line)
        XCTAssertEqual(frame.minY, expectedFrame.minY, accuracy: 0.5, file: file, line: line)
        XCTAssertEqual(frame.width, expectedFrame.width, accuracy: 0.5, file: file, line: line)
        XCTAssertEqual(frame.height, expectedFrame.height, accuracy: 0.5, file: file, line: line)
    }

    private static func makeMockWidePodLogTranscript(lineCount: Int) -> String {
        // Keep each row wide enough that the merged UTF-8 crosses the inspector’s “large payload” threshold
        // (same shape as multi-field service logs) without referencing any real workload.
        let filler = String(repeating: " f=mock", count: 260)
        return (0..<lineCount)
            .map { idx in
                "MOCK-\(String(format: "%04d", idx))Z MOCK-LEVEL 0 --- [mock-app] mock-thread mock.MockClass\(filler)"
            }
            .joined(separator: "\n")
    }

    private static func makeMockDeepPodLogTranscript(lineCount: Int, minUTF8PerLine: Int) -> String {
        let prefix = "MOCK-"
        let padLength = max(1, minUTF8PerLine - prefix.count - 8)
        let pad = String(repeating: "x", count: padLength)
        return (0..<lineCount)
            .map { idx in
                "\(prefix)\(String(format: "%05d", idx))-\(pad)"
            }
            .joined(separator: "\n")
    }
}

private final class ResourceLogsWorkLaneTracker: @unchecked Sendable {
    private let condition = NSCondition()
    private var activeCount = 0
    private var maximumActiveCountStorage = 0
    private var startedQueriesStorage: [String] = []

    func begin(query: String) {
        condition.lock()
        activeCount += 1
        maximumActiveCountStorage = max(maximumActiveCountStorage, activeCount)
        startedQueriesStorage.append(query)
        condition.broadcast()
        condition.unlock()
    }

    func end() {
        condition.lock()
        activeCount -= 1
        condition.broadcast()
        condition.unlock()
    }

    func waitUntilStarted(query: String, timeout: TimeInterval = 5) -> Bool {
        let deadline = Date().addingTimeInterval(timeout)
        condition.lock()
        defer { condition.unlock() }
        while !startedQueriesStorage.contains(query) {
            guard condition.wait(until: deadline) else { return false }
        }
        return true
    }

    var startedQueries: [String] {
        condition.lock()
        defer { condition.unlock() }
        return startedQueriesStorage
    }

    var maximumActiveCount: Int {
        condition.lock()
        defer { condition.unlock() }
        return maximumActiveCountStorage
    }
}

@MainActor
private final class ResourceLogSearchFocusModel: ObservableObject {
    let text = "INFO ready\nERROR failed\ninfo retried\nINFOrmational\n"
    @Published var query = ""
    @Published var matchCase = false
    @Published var selectedMatchIndex = 0
    @Published var focusRequestID = 0
    @Published var searchSummary: ResourceLogSearchResult?
    @Published var appearance = RuneAppearanceTheme.aurora
}

private struct ResourceLogSearchFocusHarness: View {
    @ObservedObject var model: ResourceLogSearchFocusModel

    var body: some View {
        let activeSummary = model.searchSummary?.isCurrent(
            text: model.text,
            query: model.query,
            matchCase: model.matchCase
        ) == true ? model.searchSummary : nil

        ResourceLogsSearchBar(
            query: $model.query,
            matchCase: $model.matchCase,
            selectedMatchIndex: $model.selectedMatchIndex,
            focusRequestID: model.focusRequestID,
            searchSummary: activeSummary,
            placeholder: "Search logs",
            findHelp: "Find in logs",
            matchCaseHelp: "Match case"
        )
        .runeAppearanceTheme(model.appearance.resolvedTheme)
    }
}

@MainActor
private final class ResourceLogAdjustedThemeModel: ObservableObject {
    let text = (0..<12).map { _ in "synthetic repeated error" }.joined(separator: "\n")
    @Published var theme: RuneResolvedTheme
    @Published var query = "error"
    @Published var matchCase = false
    @Published var selectedMatchIndex = 0
    @Published var selectedLogPreset = PodLogPreset.recentLines
    @Published var selectedContainer = ""

    init(theme: RuneResolvedTheme) {
        self.theme = theme
    }
}

private struct ResourceLogAdjustedThemeHarness: View {
    @ObservedObject var model: ResourceLogAdjustedThemeModel

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            ResourceLogsToolbar(
                selectedLogPreset: $model.selectedLogPreset,
                includePreviousLogs: .constant(false),
                selectedContainer: $model.selectedContainer,
                isTailModeEnabled: .constant(false),
                isStreamPaused: .constant(false),
                statusText: "Ready",
                containerOptions: ["app", "sidecar"],
                visibleLogText: model.text,
                onReload: {},
                onSave: {},
                onSaveVisibleZip: { _ in },
                onSaveFullZip: {},
                onSaveAllPodsZip: {},
                onCopySelection: {},
                onCopyAll: {},
                onToggleStreamPause: {}
            )

            ResourceLogsExplorePanel(
                searchQuery: $model.query,
                searchMatchCase: $model.matchCase,
                selectedSearchMatchIndex: $model.selectedMatchIndex,
                searchFocusRequestID: 0,
                searchResult: ResourceLogSearchResult.make(text: model.text, query: model.query),
                structuredSummary: ResourceStructuredLogAnalyzer.analyze(text: model.text),
                showsInsights: true,
                presentationStyle: .regular,
                placeholder: "Search logs",
                findHelp: "Find in logs",
                matchCaseHelp: "Match case",
                onSearchFieldSample: { _, _ in },
                onSearchDuplicate: { _ in }
            )
        }
        .runeAppearanceTheme(model.theme)
    }
}

@MainActor
private final class ResourceLogToolbarPickerModel: ObservableObject {
    @Published var selectedLogPreset = PodLogPreset.recentLines
    @Published var selectedContainer = ""
    @Published var containerOptions = ["app", "sidecar"]
}

private struct ResourceLogToolbarPickerHarness: View {
    @ObservedObject var model: ResourceLogToolbarPickerModel

    var body: some View {
        ResourceLogsToolbar(
            selectedLogPreset: $model.selectedLogPreset,
            includePreviousLogs: .constant(false),
            selectedContainer: $model.selectedContainer,
            isTailModeEnabled: .constant(false),
            isStreamPaused: .constant(false),
            statusText: "Ready",
            containerOptions: model.containerOptions,
            visibleLogText: "",
            onReload: {},
            onSave: {},
            onSaveVisibleZip: { _ in },
            onSaveFullZip: {},
            onSaveAllPodsZip: {},
            onCopySelection: {},
            onCopyAll: {},
            onToggleStreamPause: {}
        )
    }
}

@MainActor
private final class ResourceLogRenderStabilityModel: ObservableObject {
    @Published var text: String
    @Published var renderResult: ResourceLogSearchResult?
    @Published var navigationResult: ResourceLogSearchResult?
    @Published var selectedMatchIndex = 0
    @Published var navigationSequence = 0

    init(text: String, renderResult: ResourceLogSearchResult?) {
        self.text = text
        self.renderResult = renderResult
    }
}

private struct ResourceLogRenderStabilityHarness: View {
    @ObservedObject var model: ResourceLogRenderStabilityModel

    var body: some View {
        ResourceLogsOutputSurface(
            isLoadingLogs: false,
            isLoadingResources: false,
            errorMessage: nil,
            logText: model.text,
            renderSearchResult: model.renderResult,
            navigationSearchResult: model.navigationResult,
            selectedSearchMatchIndex: model.selectedMatchIndex,
            searchNavigationSequence: model.navigationSequence,
            emptyTitle: "No output",
            emptyMessage: "No synthetic output.",
            noMatchesMessage: "No synthetic matches.",
            readOnlyResetID: "renderer-stability",
            onReload: {},
            presentationStyle: .regular
        )
    }
}

private extension String {
    func slice(from start: String, to end: String) -> String? {
        guard let startRange = range(of: start),
              let endRange = range(of: end, range: startRange.upperBound..<endIndex) else {
            return nil
        }
        return String(self[startRange.lowerBound..<endRange.lowerBound])
    }
}

private extension Array {
    var only: Element? {
        count == 1 ? first : nil
    }
}
