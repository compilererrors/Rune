import AppKit
import Foundation
import struct RuneSharedCore.RuneLargeTextIndex
import RuneCore
import SwiftUI

enum ResourceLogsPresentationStyle: Hashable {
    case regular
    case terminalCompact
}

struct ResourceLogsLayoutMetrics {
    static let regularOutputMinimumHeight: CGFloat = 280
    static let terminalCompactOutputMinimumHeight: CGFloat = 210
    static let sourcePickerWidth: CGFloat = 180
    static let podPickerWidth: CGFloat = 280
    /// The log viewer header: a fixed range picker and a search field that
    /// keeps room for a useful query before the buttons drop their labels.
    static let rangePickerWidth: CGFloat = 168
    static let compactRangePickerWidth: CGFloat = 128
    /// Flexible source pickers ask for this much before the row drops its captions.
    static let sourcePickerIdealWidth: CGFloat = 220
    static let searchChromeHeight: CGFloat = RuneUILayoutMetrics.inspectorControlRowHeight
    static let searchChromeMinimumWidth: CGFloat = 220
    static let searchChromeIdealWidth: CGFloat = 420
    static let searchFieldMinimumWidth: CGFloat = 112
    static let searchFieldIdealWidth: CGFloat = 200
    static let searchMatchStatusWidth: CGFloat = 68
    static let compactSearchMatchStatusWidth: CGFloat = 52
    static let insightsRowHeight: CGFloat = RuneUILayoutMetrics.inspectorControlRowHeight
    static let querySearchDebounceNanoseconds: UInt64 = 25_000_000
    static let streamedTextSearchDebounceNanoseconds: UInt64 = 0
    static let streamedTextSummaryDebounceNanoseconds: UInt64 = 120_000_000

    static func outputMinimumHeight(for presentationStyle: ResourceLogsPresentationStyle) -> CGFloat {
        switch presentationStyle {
        case .regular:
            return regularOutputMinimumHeight
        case .terminalCompact:
            return terminalCompactOutputMinimumHeight
        }
    }

    static func startsWithStructuredSummaryCollapsed(
        for presentationStyle: ResourceLogsPresentationStyle
    ) -> Bool {
        presentationStyle == .terminalCompact
    }
}

/// An action a container adds to a log viewer's More menu (for example the
/// Terminal's "Use in Pod Shell"), instead of a button floating above it.
struct RuneLogExtraAction: Identifiable {
    let id: String
    let title: String
    let systemImage: String
    let help: String
    let action: () -> Void
}

private struct RuneLogExtraActionsKey: EnvironmentKey {
    static var defaultValue: [RuneLogExtraAction] { [] }
}

extension EnvironmentValues {
    var runeLogExtraActions: [RuneLogExtraAction] {
        get { self[RuneLogExtraActionsKey.self] }
        set { self[RuneLogExtraActionsKey.self] = newValue }
    }
}

/// Text labels when the row has room, icons with tooltips when it does not.
private struct LogHeaderLabelVisibility: ViewModifier {
    let showsLabels: Bool

    func body(content: Content) -> some View {
        if showsLabels {
            content.labelStyle(.titleAndIcon)
        } else {
            content.labelStyle(.iconOnly)
        }
    }
}

/// A control with a short caption in front of it, so a picker says what it
/// picks ("Pod", "Range") without a label row above it.
struct LogHeaderLabeledField<Content: View>: View {
    let title: String
    var flexible = true
    var showsCaption = true
    @ViewBuilder let content: Content

    init(_ title: String, flexible: Bool = true, showsCaption: Bool = true, @ViewBuilder content: () -> Content) {
        self.title = title
        self.flexible = flexible
        self.showsCaption = showsCaption
        self.content = content()
    }

    var body: some View {
        HStack(alignment: .center, spacing: RuneSpacing.tight) {
            if showsCaption {
                Text(title)
                    .runeText(.caption)
                    .foregroundStyle(.runeSecondary)
                    .lineLimit(1)
                    .fixedSize()
            }
            if flexible {
                content
                    .frame(
                        minWidth: 0,
                        idealWidth: ResourceLogsLayoutMetrics.sourcePickerIdealWidth,
                        maxWidth: .infinity,
                        alignment: .leading
                    )
            } else {
                content
            }
        }
        .frame(maxWidth: flexible ? .infinity : nil, alignment: .leading)
        .help(showsCaption ? "" : title)
        .accessibilityElement(children: .contain)
        .accessibilityLabel(title)
    }
}

struct ResourceLogsToolbar: View {
    @Binding var selectedLogPreset: PodLogPreset
    @Binding var includePreviousLogs: Bool
    @Binding var selectedContainer: String
    @Binding var isTailModeEnabled: Bool
    @Binding var isStreamPaused: Bool
    let statusText: String
    var podOptions: [PodSummary] = []
    var selectedPodID: Binding<String>? = nil
    var isFavoritePod: (PodSummary) -> Bool = { _ in false }
    var onToggleFavoritePod: (PodSummary) -> Void = { _ in }
    var presentationStyle: ResourceLogsPresentationStyle = .regular
    var showsContainerPicker: Bool = true
    let containerOptions: [String]
    var sourceSummaryTitle: String? = nil
    var sourceSummaryValues: [String] = []
    let visibleLogText: String
    let onReload: () -> Void
    let onSave: () -> Void
    var onSaveToExportFolder: () -> Void = {}
    var onSaveAndOpen: () -> Void = {}
    let onSaveVisibleZip: (String) -> Void
    var onSaveVisibleZipToExportFolder: (String) -> Void = { _ in }
    var onSaveVisibleZipAndOpen: (String) -> Void = { _ in }
    let onSaveFullZip: () -> Void
    var onSaveFullZipToExportFolder: () -> Void = {}
    var onSaveFullZipAndOpen: () -> Void = {}
    let onSaveAllPodsZip: () -> Void
    var onSaveAllPodsZipToExportFolder: () -> Void = {}
    var onSaveAllPodsZipAndOpen: () -> Void = {}
    let onCopySelection: () -> Void
    let onCopyAll: () -> Void
    let onToggleStreamPause: () -> Void
    var interfaceLanguageRaw: String = RuneSettingsKeys.interfaceLanguageDefault
    /// The log search field, placed in the header's action row when the
    /// toolbar heads a log document.
    var searchField: AnyView? = nil
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @Environment(\.runeInterfaceFontSize) private var interfaceFontSize
    @Environment(\.runeDocumentFrameEmbedded) private var isEmbedded
    @Environment(\.runeLogExtraActions) private var extraActions

    var body: some View {
        if isEmbedded {
            logViewerHeader
        } else {
            switch presentationStyle {
            case .regular:
                regularBody
            case .terminalCompact:
                terminalCompactBody
            }
        }
    }

    // MARK: Log viewer header (Design/README.md, "Log viewer")

    /// Two rows on one grid, as in Console, Lens and Loki/Datadog explorers:
    /// the source (pod or container, and the time range) on the first row;
    /// Follow, Reload, the search field and the Save and More menus on the
    /// second. Labels show whenever they fit; at narrow widths the same row
    /// switches to icons with tooltips, so the header never grows a row.
    private var logViewerHeader: some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.documentFrameBarVerticalInset) {
            ViewThatFits(in: .horizontal) {
                sourceHeaderRow(showsCaptions: true)
                sourceHeaderRow(showsCaptions: false)
            }
            // The first form that fits the bar's width, from its ideal size,
            // with no measured-width state that could feed back into layout.
            // Buttons keep their own width, so a form fits only when the
            // search field still has its minimum beside them.
            ViewThatFits(in: .horizontal) {
                actionHeaderRow(showsLabels: true)
                actionHeaderRow(showsLabels: false)
                // A narrow inspector: the buttons on one line, search below.
                VStack(alignment: .leading, spacing: RuneUILayoutMetrics.documentFrameBarVerticalInset) {
                    actionButtonsRow
                    searchSlot
                }
            }
            .frame(minWidth: 0, maxWidth: .infinity, alignment: .leading)
        }
        .controlSize(toolbarControlSize)
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Log controls")
    }

    private func sourceHeaderRow(showsCaptions: Bool) -> some View {
        HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarControlSpacing) {
            if let selectedPodID, !podOptions.isEmpty {
                LogHeaderLabeledField(t(.pod), showsCaption: showsCaptions) {
                    FavoritePodPicker(
                        title: t(.pod),
                        pods: podOptions,
                        width: ResourceLogsLayoutMetrics.podPickerWidth,
                        rowTitle: { $0.name },
                        rowDetail: { "\($0.namespace) - \($0.status)" },
                        isFavoritePod: isFavoritePod,
                        onToggleFavoritePod: onToggleFavoritePod,
                        selection: selectedPodID,
                        fillsWidth: true,
                        showsFavoriteButton: false
                    )
                    .accessibilityIdentifier("pod-log-favorite-picker")
                }
            }

            if showsContainerPicker, !containerOptions.isEmpty {
                LogHeaderLabeledField(t(.container), showsCaption: showsCaptions) {
                    LogToolbarPopupPicker(
                        "Container",
                        selection: $selectedContainer,
                        options: [
                            .init(value: "", title: t(.allContainers))
                        ] + containerOptions.map { container in
                            .init(value: container, title: container)
                        }
                    )
                    .modifier(RuneToolbarControlSurface(fillsWidth: true))
                    .help(t(.chooseContainerHelp))
                }
            }

            if let sourceSummaryTitle, !sourceSummaryValues.isEmpty {
                LogHeaderLabeledField(sourceSummaryTitle, showsCaption: showsCaptions) {
                    Text(sourceSummaryValues.joined(separator: ", "))
                        .runeText(.caption)
                        .lineLimit(1)
                        .truncationMode(.middle)
                        .help("\(sourceSummaryTitle): \(sourceSummaryValues.joined(separator: ", "))")
                }
            }

            // Range follows the source; alone, it starts the row.
            LogHeaderLabeledField("Range", flexible: false, showsCaption: showsCaptions) {
                LogToolbarPopupPicker(
                    "Log range",
                    selection: $selectedLogPreset,
                    options: PodLogPreset.allCases.map { preset in
                        .init(value: preset, title: preset.title)
                    }
                )
                .modifier(RuneToolbarControlSurface(fillsWidth: true))
                .frame(width: showsCaptions ? ResourceLogsLayoutMetrics.rangePickerWidth : ResourceLogsLayoutMetrics.compactRangePickerWidth)
                .help("Range: how much log history to load")
            }
        }
        .frame(maxWidth: .infinity, minHeight: RuneControlHeight.toolbar, alignment: .leading)
    }

    private var hasSourcePickers: Bool {
        (selectedPodID != nil && !podOptions.isEmpty)
            || (showsContainerPicker && !containerOptions.isEmpty)
            || (sourceSummaryTitle != nil && !sourceSummaryValues.isEmpty)
    }

    private func actionHeaderRow(showsLabels: Bool) -> some View {
        HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarControlSpacing) {
            followButton
            reloadButton
            searchSlot
                .layoutPriority(1)
            saveMenu
            moreMenu
        }
        .modifier(LogHeaderLabelVisibility(showsLabels: showsLabels))
        .fixedSize(horizontal: false, vertical: true)
        .frame(maxWidth: .infinity, minHeight: RuneControlHeight.toolbar, alignment: .leading)
    }

    /// The narrow form: the buttons on one line (icons), search below.
    private var actionButtonsRow: some View {
        HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarControlSpacing) {
            followButton
            reloadButton
            Spacer(minLength: 0)
            saveMenu
            moreMenu
        }
        .modifier(LogHeaderLabelVisibility(showsLabels: false))
        .frame(maxWidth: .infinity, minHeight: RuneControlHeight.toolbar, alignment: .leading)
    }

    private var reloadButton: some View {
        Button(action: onReload) {
            Label(t(.reload), systemImage: "arrow.clockwise")
        }
        .buttonStyle(RuneToolbarButtonStyle())
        // The search field has layout priority; buttons keep their width.
        .fixedSize(horizontal: true, vertical: false)
        .help(t(.reloadLogsHelp))
        .accessibilityLabel(t(.reload))
    }

    /// The search field fills the space between the actions; its ideal width
    /// is its real minimum, so the row picks labels only when they truly fit.
    @ViewBuilder
    private var searchSlot: some View {
        if let searchField {
            searchField
                .frame(
                    minWidth: ResourceLogsLayoutMetrics.searchChromeMinimumWidth,
                    idealWidth: ResourceLogsLayoutMetrics.searchChromeMinimumWidth,
                    maxWidth: .infinity
                )
        } else {
            Spacer(minLength: 0)
        }
    }

    /// Follow (live tail) as a labelled toggle with a visible state, as in
    /// Console's streaming control and Loki's Live: "Follow" when off,
    /// "Following" while streaming, "Paused" when paused. Accessibility keeps
    /// the action name and the stream status.
    private var followButton: some View {
        Button(action: toggleTailPlayback) {
            Label {
                RuneStableLabel(followTitle, reserving: ["Follow", "Following", "Paused"])
            } icon: {
                Image(systemName: followSymbol)
            }
        }
        .buttonStyle(RuneToolbarButtonStyle(isSelected: isTailModeEnabled && !isStreamPaused))
        .fixedSize(horizontal: true, vertical: false)
        .help("\(statusText). \(tailActionHelp)")
        .accessibilityLabel(tailActionTitle)
        .accessibilityValue(statusText)
        .accessibilityIdentifier("log-tail-playback")
        .contextMenu {
            if isTailModeEnabled {
                Button(t(.stopTail)) {
                    isTailModeEnabled = false
                    isStreamPaused = false
                }
            }
        }
    }

    private var followTitle: String {
        guard isTailModeEnabled else { return "Follow" }
        return isStreamPaused ? "Paused" : "Following"
    }

    private var followSymbol: String {
        guard isTailModeEnabled else { return "dot.radiowaves.left.and.right" }
        return isStreamPaused ? "pause.fill" : "dot.radiowaves.left.and.right"
    }

    /// Every way to keep the logs, in one menu.
    private var saveMenu: some View {
        RuneToolbarMenu {
            Button(action: onSave) {
                Label("\(t(.saveLogs))…", systemImage: "square.and.arrow.down")
            }
            Button(action: onSaveToExportFolder) {
                Label("Save to Export Folder", systemImage: "folder.badge.plus")
            }
            .accessibilityIdentifier("log-quick-save")
            Button(action: onSaveAndOpen) {
                Label("Save and Open", systemImage: "arrow.up.right.square")
            }

            Divider()

            Menu {
                Button("Save As…") { onSaveVisibleZip(visibleLogText) }
                Button("Save to Export Folder") { onSaveVisibleZipToExportFolder(visibleLogText) }
                Button("Save and Open") { onSaveVisibleZipAndOpen(visibleLogText) }
            } label: {
                Label(t(.exportVisibleResultsZip), systemImage: "doc.zipper")
            }
            Menu {
                Button("Save As…", action: onSaveFullZip)
                Button("Save to Export Folder", action: onSaveFullZipToExportFolder)
                Button("Save and Open", action: onSaveFullZipAndOpen)
            } label: {
                Label(t(.exportFullUnfilteredZip), systemImage: "archivebox")
            }
            Menu {
                Button("Save As…", action: onSaveAllPodsZip)
                Button("Save to Export Folder", action: onSaveAllPodsZipToExportFolder)
                Button("Save and Open", action: onSaveAllPodsZipAndOpen)
            } label: {
                Label(t(.exportAllPodsFullZip), systemImage: "shippingbox")
            }
        } label: {
            Label("Save", systemImage: "square.and.arrow.down")
        }
        .help(t(.saveCurrentLogsHelp))
        .accessibilityLabel(t(.saveLogs))
    }

    /// Rare actions: copying, previous-container logs and hand-offs.
    private var moreMenu: some View {
        RuneToolbarMenu {
            Button(action: onCopySelection) {
                Label(t(.copySelection), systemImage: "doc.on.doc")
            }
            Button(action: onCopyAll) {
                Label(t(.copyAll), systemImage: "doc.on.clipboard")
            }

            Divider()

            Toggle(isOn: $includePreviousLogs) {
                Label("Include Previous Container", systemImage: "clock.arrow.circlepath")
            }
            .help(t(.previousLogsHelp))

            if !extraActions.isEmpty {
                Divider()
                ForEach(extraActions) { action in
                    Button(action: action.action) {
                        Label(action.title, systemImage: action.systemImage)
                    }
                    .help(action.help)
                    .accessibilityIdentifier(action.id)
                }
            }
        } label: {
            Label(t(.more), systemImage: "ellipsis.circle")
                .labelStyle(.iconOnly)
        }
        .help("Copy, previous container logs and more")
        .accessibilityLabel(t(.more))
    }

    /// Sources on one row, actions on the next, at every width, so resizing
    /// the pane never changes the bar's height. Inside the log document's
    /// header bar the groups have no cards.
    private var regularBody: some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.inspectorControlRowSpacing) {
            sourceRow
                .controlSize(toolbarControlSize)

            toolbarActions
                .controlSize(toolbarControlSize)
        }
    }

    private var terminalCompactBody: some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.inspectorCompactSectionSpacing) {
            sourceRow
                .controlSize(toolbarControlSize)

            toolbarActions
                .controlSize(toolbarControlSize)
        }
    }

    private var toolbarControlSize: ControlSize {
        if dynamicTypeSize.isAccessibilitySize
            || interfaceFontSize > RuneInterfaceTypography.standardMenuFontSize + 1 {
            return .regular
        }
        return .small
    }

    private var sourceControls: some View {
        LogToolbarGroup(role: .source) {
            sourceItems
        }
    }

    /// The source pickers as separate items, so a narrow bar wraps them one
    /// by one instead of clipping a picker at the frame edge.
    @ViewBuilder
    private var sourceItems: some View {
            LogToolbarPickerField(title: t(.window), role: .window) {
                LogToolbarPopupPicker(
                    "Log window",
                    selection: $selectedLogPreset,
                    options: PodLogPreset.allCases.map { preset in
                        .init(value: preset, title: preset.title)
                    }
                )
                .modifier(RuneToolbarControlSurface(fillsWidth: true))
                .frame(width: isEmbedded ? nil : ResourceLogsLayoutMetrics.sourcePickerWidth)
            }

            if let selectedPodID, !podOptions.isEmpty {
                LogToolbarPickerField(title: t(.pod), role: .pod) {
                    FavoritePodPicker(
                        title: t(.pod),
                        pods: podOptions,
                        width: ResourceLogsLayoutMetrics.podPickerWidth,
                        rowTitle: { $0.name },
                        rowDetail: { "\($0.namespace) - \($0.status)" },
                        isFavoritePod: isFavoritePod,
                        onToggleFavoritePod: onToggleFavoritePod,
                        selection: selectedPodID,
                        fillsWidth: isEmbedded
                    )
                    .accessibilityIdentifier("pod-log-favorite-picker")
                }
            }

            if showsContainerPicker, !containerOptions.isEmpty {
                LogToolbarPickerField(title: t(.container), role: .container) {
                    LogToolbarPopupPicker(
                        "Container",
                        selection: $selectedContainer,
                        options: [
                            .init(value: "", title: t(.allContainers))
                        ] + containerOptions.map { container in
                            .init(value: container, title: container)
                        }
                    )
                    .modifier(RuneToolbarControlSurface(fillsWidth: true))
                    .frame(width: isEmbedded ? nil : ResourceLogsLayoutMetrics.sourcePickerWidth)
                    .help(t(.chooseContainerHelp))
                }
            }

            if let sourceSummaryTitle, !sourceSummaryValues.isEmpty {
                LogToolbarSourceSummary(
                    title: sourceSummaryTitle,
                    values: sourceSummaryValues
                )
            }
    }

    /// In the log document's bar the pickers share the full width equally;
    /// on their own they keep fixed widths and scroll.
    @ViewBuilder
    private var sourceRow: some View {
        if isEmbedded {
            HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarGroupSpacing) {
                sourceItems
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        } else {
            LogToolbarScrollRow {
                primaryControls
            }
        }
    }

    private var primaryControls: some View {
        HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarGroupSpacing) {
            sourceControls
        }
        .fixedSize(horizontal: true, vertical: false)
    }

    private var tailControl: some View {
        Button(action: toggleTailPlayback) {
            LogToolbarStatusIndicator(
                statusText: statusText,
                isTailModeEnabled: isTailModeEnabled,
                isStreamPaused: isStreamPaused
            )
        }
        .buttonStyle(RuneToolbarButtonStyle(isIconOnly: true))
        .help("\(statusText). \(tailActionHelp)")
        .accessibilityLabel(tailActionTitle)
        .accessibilityValue(statusText)
        .accessibilityIdentifier("log-tail-playback")
        .contextMenu {
            if isTailModeEnabled {
                Button(t(.stopTail)) {
                    isTailModeEnabled = false
                    isStreamPaused = false
                }
            }
        }
    }

    private var tailActionTitle: String {
        guard isTailModeEnabled else { return t(.tail) }
        return isStreamPaused ? t(.resume) : t(.pause)
    }

    private var tailActionHelp: String {
        guard isTailModeEnabled else { return t(.startTailHelp) }
        return isStreamPaused ? t(.resumeTailHelp) : t(.pauseTailHelp)
    }

    private func toggleTailPlayback() {
        if isTailModeEnabled {
            onToggleStreamPause()
        } else {
            isTailModeEnabled = true
            isStreamPaused = false
        }
    }

    private var toolbarActions: some View {
        ViewThatFits(in: .horizontal) {
            if isEmbedded {
                // Playback on the left, saving on the right, across the full bar.
                HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarControlSpacing) {
                    LogToolbarGroup { playbackActions }
                    Spacer(minLength: RuneUILayoutMetrics.inspectorToolbarControlSpacing)
                    LogToolbarGroup { exportActions }
                }
                .frame(maxWidth: .infinity)
            } else {
                LogToolbarGroup {
                    playbackActions
                    exportActions
                }
            }
            VStack(alignment: .leading, spacing: RuneUILayoutMetrics.inspectorWrappedActionRowSpacing) {
                LogToolbarScrollRow {
                    LogToolbarGroup { playbackActions }
                }
                LogToolbarScrollRow {
                    LogToolbarGroup { exportActions }
                }
            }
        }
    }

    @ViewBuilder
    private var playbackActions: some View {
        tailControl

        Button(action: onReload) {
            Label(t(.reload), systemImage: "arrow.clockwise")
        }
        .buttonStyle(RuneToolbarButtonStyle())
        .logToolbarButtonFrame()
        .help(t(.reloadLogsHelp))

        Toggle(isOn: $includePreviousLogs) {
            toolbarIconLabel(t(.previous), systemImage: "clock.arrow.circlepath", help: t(.previousLogsHelp))
        }
        .toggleStyle(RuneToolbarToggleStyle(isIconOnly: true))
        .logToolbarIconButtonFrame()
        .help(t(.previousLogsHelp))
        .accessibilityLabel(t(.previous))
    }

    @ViewBuilder
    private var exportActions: some View {
        Button(action: onSave) {
            Label(t(.saveLogs), systemImage: "square.and.arrow.down")
        }
        .buttonStyle(RuneToolbarButtonStyle())
        .logToolbarButtonFrame()
        .help(t(.saveCurrentLogsHelp))

        Button(action: onSaveToExportFolder) {
            toolbarIconLabel(t(.quickSaveLogs), systemImage: "folder.badge.plus", help: t(.quickSaveLogsHelp))
        }
        .buttonStyle(RuneToolbarButtonStyle(isIconOnly: true))
        .logToolbarIconButtonFrame()
        .help(t(.quickSaveLogsHelp))
        .accessibilityLabel(t(.quickSaveLogs))
        .accessibilityIdentifier("log-quick-save")

        RuneToolbarMenu {
            Button(action: onCopySelection) {
                Label(t(.copySelection), systemImage: "doc.on.doc")
            }
            Button(action: onCopyAll) {
                Label(t(.copyAll), systemImage: "doc.on.clipboard")
            }

            Divider()

            Menu {
                Button(action: onSaveToExportFolder) {
                    Label("Save to Export Folder", systemImage: "folder")
                }
                Button(action: onSaveAndOpen) {
                    Label("Save and Open", systemImage: "arrow.up.right.square")
                }
            } label: {
                Label("Current Logs", systemImage: "doc.text")
            }

            Divider()

            Menu {
                Button {
                    onSaveVisibleZip(visibleLogText)
                } label: {
                    Label("Save As...", systemImage: "doc.zipper")
                }
                Button {
                    onSaveVisibleZipToExportFolder(visibleLogText)
                } label: {
                    Label("Save to Export Folder", systemImage: "folder.badge.plus")
                }
                Button {
                    onSaveVisibleZipAndOpen(visibleLogText)
                } label: {
                    Label("Save and Open", systemImage: "archivebox")
                }
            } label: {
                Label(t(.exportVisibleResultsZip), systemImage: "doc.zipper")
            }

            Menu {
                Button(action: onSaveFullZip) {
                    Label("Save As...", systemImage: "archivebox")
                }
                Button(action: onSaveFullZipToExportFolder) {
                    Label("Save to Export Folder", systemImage: "folder.badge.plus")
                }
                Button(action: onSaveFullZipAndOpen) {
                    Label("Save and Open", systemImage: "archivebox")
                }
            } label: {
                Label(t(.exportFullUnfilteredZip), systemImage: "archivebox")
            }

            Menu {
                Button(action: onSaveAllPodsZip) {
                    Label("Save As...", systemImage: "shippingbox")
                }
                Button(action: onSaveAllPodsZipToExportFolder) {
                    Label("Save to Export Folder", systemImage: "folder.badge.plus")
                }
                Button(action: onSaveAllPodsZipAndOpen) {
                    Label("Save and Open", systemImage: "archivebox")
                }
            } label: {
                Label(t(.exportAllPodsFullZip), systemImage: "shippingbox")
            }
        } label: {
            Label(t(.more), systemImage: "ellipsis.circle")
                .labelStyle(.iconOnly)
        }
        .help(t(.copyAndExportLogOutputHelp))
        .accessibilityLabel(t(.more))
    }

    private var language: RuneLanguage {
        RuneLanguage.resolved(interfaceLanguageRaw)
    }

    private func t(_ key: RuneLocalizedStringKey) -> String {
        RuneLocalizedStrings.shared.string(key, language: language)
    }

    private func toolbarIconLabel(_ title: String, systemImage: String, help: String) -> some View {
        Label(title, systemImage: systemImage)
            .help(help)
            .accessibilityLabel(title)
    }
}

private extension View {
    func logToolbarButtonFrame() -> some View {
        labelStyle(.titleAndIcon)
            .fixedSize(horizontal: true, vertical: false)
    }

    func logToolbarIconButtonFrame() -> some View {
        labelStyle(.iconOnly)
            .fixedSize(horizontal: true, vertical: false)
    }
}

private enum LogToolbarGroupRole {
    case source
    case action

    var height: CGFloat {
        switch self {
        case .source:
            return RuneUILayoutMetrics.inspectorToolbarSourceGroupHeight
        case .action:
            return RuneUILayoutMetrics.inspectorToolbarActionGroupHeight
        }
    }

    var verticalPadding: CGFloat {
        switch self {
        case .source:
            return RuneUILayoutMetrics.inspectorToolbarGroupVerticalPadding
        case .action:
            return RuneUILayoutMetrics.inspectorControlSurfaceVerticalPadding
        }
    }
}

private struct LogToolbarGroup<Content: View>: View {
    var role: LogToolbarGroupRole = .action
    var spacing: CGFloat = RuneUILayoutMetrics.inspectorToolbarControlSpacing
    @ViewBuilder let content: Content
    @Environment(\.runeThemePalette) private var runeThemePalette

    @Environment(\.runeDocumentFrameEmbedded) private var isEmbedded

    var body: some View {
        // In the log document's header bar the bar is the container: no card.
        if isEmbedded {
            HStack(alignment: .center, spacing: spacing) {
                content
            }
            .fixedSize(horizontal: true, vertical: false)
        } else {
            HStack(alignment: .center, spacing: spacing) {
                content
            }
            .padding(.horizontal, RuneUILayoutMetrics.inspectorToolbarGroupHorizontalPadding)
            .padding(.vertical, role.verticalPadding)
            .frame(minHeight: role.height)
            .modifier(RuneCardSurface())
            .fixedSize(horizontal: true, vertical: false)
        }
    }
}

private struct LogToolbarScrollRow<Content: View>: View {
    @ViewBuilder let content: Content

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarGroupSpacing) {
                content
            }
            .fixedSize(horizontal: true, vertical: false)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}

private struct LogToolbarPickerField<Content: View>: View {
    enum Role {
        case window
        case pod
        case container
    }

    let title: String
    let role: Role
    @ViewBuilder let content: Content
    @Environment(\.runeThemePalette) private var runeThemePalette

    @Environment(\.runeDocumentFrameEmbedded) private var isEmbedded

    var body: some View {
        if isEmbedded {
            // In a bar the picker's own value names it; the title stays as
            // help, and the pickers share the bar's width.
            content
                .help(title)
                .frame(minWidth: 0, maxWidth: .infinity, alignment: .leading)
        } else {
            labelledBody
        }
    }

    private var labelledBody: some View {
        VStack(alignment: .leading, spacing: RuneSpacing.tight) {
            Text(title.uppercased())
                .runeInterfaceFont(relativeSize: -2, weight: .bold)
                .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
                .lineLimit(1)
                .tracking(0.3)
            content
        }
        .frame(width: fieldWidth, alignment: .leading)
        .fixedSize(horizontal: true, vertical: false)
    }

    private var fieldWidth: CGFloat {
        switch role {
        case .pod:
            return ResourceLogsLayoutMetrics.podPickerWidth
        case .container, .window:
            return ResourceLogsLayoutMetrics.sourcePickerWidth
        }
    }

}

struct LogToolbarPopupPicker<Value: Hashable>: NSViewRepresentable {
    struct Option: Hashable {
        let value: Value
        let title: String
    }

    let accessibilityLabel: String
    @Binding var selection: Value
    let options: [Option]
    @Environment(\.runeInterfaceFontSize) private var interfaceMenuFontSize

    init(
        _ accessibilityLabel: String,
        selection: Binding<Value>,
        options: [Option]
    ) {
        self.accessibilityLabel = accessibilityLabel
        _selection = selection
        self.options = options
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(parent: self)
    }

    func makeNSView(context: Context) -> NSPopUpButton {
        let popup = NSPopUpButton(frame: .zero, pullsDown: false)
        context.coordinator.attach(to: popup)
        return popup
    }

    func updateNSView(_ popup: NSPopUpButton, context: Context) {
        context.coordinator.update(parent: self, popup: popup)
    }

    /// The same 13 pt medium label as every toolbar control.
    private var popupFont: NSFont {
        NSFont.systemFont(
            ofSize: interfaceMenuFontSize + RuneControlSize.large.labelRelativeSize,
            weight: .medium
        )
    }

    @MainActor
    final class Coordinator: NSObject, NSMenuDelegate {
        private struct Projection {
            let options: [Option]
            let selection: Value
        }

        var parent: LogToolbarPopupPicker
        private(set) var displayedOptions: [Option] = []
        private var trackedOptions: [Option] = []
        private var deferredProjection: Projection?
        private var pendingUserSelection: Value?
        private var isMenuTracking = false
        private var isAwaitingMenuAction = false
        private var menuCloseGeneration: UInt64 = 0
        private weak var popup: NSPopUpButton?

        init(parent: LogToolbarPopupPicker) {
            self.parent = parent
        }

        func attach(to popup: NSPopUpButton) {
            self.popup = popup
            // Drawn on the shared toolbar surface (RuneToolbarControlSurface):
            // the popup itself is borderless and keeps only its title and arrows.
            popup.controlSize = .regular
            popup.isBordered = false
            (popup.cell as? NSPopUpButtonCell)?.arrowPosition = .arrowAtCenter
            popup.setContentHuggingPriority(.defaultLow, for: .horizontal)
            popup.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
            popup.target = self
            popup.action = #selector(selectionChanged(_:))
            popup.menu?.delegate = self
            update(parent: parent, popup: popup)
        }

        func update(parent: LogToolbarPopupPicker, popup: NSPopUpButton) {
            self.parent = parent
            self.popup = popup
            popup.menu?.delegate = self
            popup.font = parent.popupFont
            popup.setAccessibilityLabel(parent.accessibilityLabel)

            let projection = Projection(
                options: parent.options,
                selection: parent.selection
            )
            guard !isMenuTracking, !isAwaitingMenuAction else {
                deferredProjection = projection
                return
            }
            apply(projection, to: popup)
        }

        func menuWillOpen(_ menu: NSMenu) {
            guard let popup, popup.menu === menu else { return }
            if isAwaitingMenuAction {
                finishDeferredProjection(on: popup)
            }
            menuCloseGeneration &+= 1
            isMenuTracking = true
            isAwaitingMenuAction = false
            trackedOptions = displayedOptions
            deferredProjection = nil
        }

        func menuDidClose(_ menu: NSMenu) {
            guard let popup, popup.menu === menu else { return }
            isMenuTracking = false
            isAwaitingMenuAction = true
            menuCloseGeneration &+= 1
            let generation = menuCloseGeneration

            // AppKit can close the menu before delivering its target/action. Waiting one
            // main-loop turn keeps the displayed option snapshot alive for that action.
            DispatchQueue.main.async { [weak self, weak popup] in
                guard let self, let popup,
                      self.menuCloseGeneration == generation,
                      self.isAwaitingMenuAction else {
                    return
                }
                self.finishDeferredProjection(on: popup)
            }
        }

        @objc func selectionChanged(_ sender: NSPopUpButton) {
            let index = sender.indexOfSelectedItem
            let actionOptions = (isMenuTracking || isAwaitingMenuAction)
                ? trackedOptions
                : displayedOptions
            guard actionOptions.indices.contains(index) else { return }
            let option = actionOptions[index]
            setSelectedOptionHelp(option.title, on: sender)
            let value = option.value
            pendingUserSelection = value
            parent.selection = value
        }

        func finishDeferredProjection(on popup: NSPopUpButton) {
            menuCloseGeneration &+= 1
            isMenuTracking = false
            isAwaitingMenuAction = false
            let projection = deferredProjection ?? Projection(
                options: parent.options,
                selection: parent.selection
            )
            deferredProjection = nil
            apply(projection, to: popup)
            trackedOptions = []
        }

        private func apply(_ projection: Projection, to popup: NSPopUpButton) {
            let protectedSelection: Value
            if let pendingUserSelection {
                if projection.selection == pendingUserSelection {
                    protectedSelection = pendingUserSelection
                    self.pendingUserSelection = nil
                } else if projection.options.contains(where: { $0.value == pendingUserSelection }) {
                    protectedSelection = pendingUserSelection
                } else {
                    protectedSelection = projection.selection
                    self.pendingUserSelection = nil
                }
            } else {
                protectedSelection = projection.selection
            }

            let titles = projection.options.map(\.title)
            if popup.itemTitles != titles {
                popup.removeAllItems()
                popup.addItems(withTitles: titles)
            }
            displayedOptions = projection.options
            if let selectedIndex = projection.options.firstIndex(where: {
                $0.value == protectedSelection
            }) {
                popup.selectItem(at: selectedIndex)
                setSelectedOptionHelp(projection.options[selectedIndex].title, on: popup)
            } else {
                popup.select(nil)
                setSelectedOptionHelp(nil, on: popup)
            }
            popup.isEnabled = !projection.options.isEmpty
        }

        private func setSelectedOptionHelp(_ text: String?, on popup: NSPopUpButton) {
            popup.toolTip = text
            popup.setAccessibilityHelp(text)
        }
    }
}

private struct LogToolbarStatusIndicator: View {
    let statusText: String
    let isTailModeEnabled: Bool
    let isStreamPaused: Bool
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        Image(systemName: isTailModeEnabled && !isStreamPaused ? "pause.fill" : "play.fill")
            .foregroundStyle(statusColor)
            .overlay(alignment: .topTrailing) {
                Circle()
                    .fill(statusColor)
                    .frame(width: 4, height: 4)
                    .offset(x: 5, y: -3)
                    .accessibilityHidden(true)
            }
    }

    private var statusColor: Color {
        let normalized = statusText.lowercased()
        if normalized.contains("failed") || normalized.contains("error") {
            return RuneSemanticColorRole.danger.color(in: runeThemePalette)
        }
        if normalized.contains("loading") || normalized.contains("reloading") {
            return RuneSemanticColorRole.info.color(in: runeThemePalette)
        }
        if isTailModeEnabled && isStreamPaused {
            return RuneSemanticColorRole.warning.color(in: runeThemePalette)
        }
        guard isTailModeEnabled else {
            return runeThemePalette?.secondaryText ?? Color.secondary
        }
        return RuneSemanticColorRole.success.color(in: runeThemePalette)
    }
}

private struct LogToolbarSourceSummary: View {
    let title: String
    let values: [String]
    @Environment(\.runeThemePalette) private var runeThemePalette

    @Environment(\.runeDocumentFrameEmbedded) private var isEmbedded

    var body: some View {
        if isEmbedded {
            // In a bar the values speak for themselves; the title stays as help.
            sourceLabel
                .frame(minWidth: 0, maxWidth: .infinity, alignment: .leading)
                .accessibilityLabel("\(title): \(joinedValues)")
        } else {
            labelledBody
        }
    }

    private var sourceLabel: some View {
        Label(joinedValues, systemImage: "square.stack.3d.forward.dottedline")
            .runeInterfaceFont(relativeSize: -1, weight: .medium)
            .foregroundStyle(runeThemePalette?.foreground ?? Color.primary)
            .lineLimit(1)
            .truncationMode(.middle)
            .help("\(title): \(joinedValues)")
    }

    private var labelledBody: some View {
        VStack(alignment: .leading, spacing: RuneSpacing.tight) {
            Text(title.uppercased())
                .runeInterfaceFont(relativeSize: -2, weight: .bold)
                .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
                .lineLimit(1)
                .tracking(0.3)

            Label(joinedValues, systemImage: "square.stack.3d.forward.dottedline")
                .runeInterfaceFont(relativeSize: -1, weight: .medium)
                .foregroundStyle(runeThemePalette?.foreground ?? Color.primary)
                .lineLimit(1)
                .truncationMode(.middle)
                .help("\(title): \(joinedValues)")
        }
        .frame(width: 240, alignment: .leading)
        .fixedSize(horizontal: true, vertical: false)
        .accessibilityLabel("\(title): \(joinedValues)")
    }

    private var joinedValues: String {
        values.joined(separator: ", ")
    }
}

struct ResourceStructuredLogSummaryPanel: View {
    let summary: ResourceStructuredLogSummary
    let onSearchFieldSample: (ResourceStructuredLogField, String) -> Void
    let onSearchDuplicate: (ResourceDuplicateLogLine) -> Void
    var presentationStyle: ResourceLogsPresentationStyle = .regular
    @State private var isCompactSummaryExpanded = false
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        if hasInsights {
            summaryContent
                .accessibilityLabel(summarySubtitle)
        }
    }

    var hasInsights: Bool {
        summary.isStructured || !summary.duplicateLines.isEmpty
    }

    @ViewBuilder
    private var summaryContent: some View {
        if ResourceLogsLayoutMetrics.startsWithStructuredSummaryCollapsed(for: presentationStyle) {
            RuneDisclosureSection(
                "\(summary.isStructured ? "Structured logs" : "Repeated lines"), \(summarySubtitle)",
                isExpanded: $isCompactSummaryExpanded
            ) {
                summaryDetails
                    .padding(.top, RuneSpacing.xs)
            } label: {
                summaryHeader
            }
        } else {
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(alignment: .center, spacing: RuneSpacing.xs) {
                    summaryHeader
                    summaryControlContent
                }
                .fixedSize(horizontal: true, vertical: false)
            }
            .frame(
                maxWidth: .infinity,
                minHeight: ResourceLogsLayoutMetrics.insightsRowHeight,
                maxHeight: ResourceLogsLayoutMetrics.insightsRowHeight,
                alignment: .leading
            )
        }
    }

    private var summaryHeader: some View {
        RuneInspectorControlGridRow {
            Image(systemName: summary.isStructured ? "curlybraces" : "text.line.first.and.arrowtriangle.forward")
                .runeText(.microEmphasis)
                .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
        } content: {
            HStack(alignment: .center, spacing: RuneSpacing.xs) {
                Text(summary.isStructured ? "Structured logs" : "Repeated lines")
                    .runeText(.microEmphasis)
                    .foregroundStyle(runeThemePalette?.foreground ?? Color.primary)

                Text(summarySubtitle)
                    .runeText(.micro)
                    .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
                    .lineLimit(1)
                    .truncationMode(.middle)
            }
        }
        .fixedSize(horizontal: true, vertical: false)
    }

    private var summaryDetails: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(alignment: .center, spacing: RuneSpacing.tight) {
                summaryControlContent
            }
            .fixedSize(horizontal: true, vertical: false)
        }
        .frame(minHeight: ResourceLogsLayoutMetrics.insightsRowHeight, alignment: .leading)
    }

    @ViewBuilder
    private var summaryControlContent: some View {
        ForEach(summary.fields.prefix(6), id: \.key) { field in
            fieldMenu(field)
        }

        if !summary.fields.isEmpty, !summary.duplicateLines.isEmpty {
            Rectangle()
                .fill(runeThemePalette?.divider ?? Color(nsColor: .separatorColor).opacity(0.35))
                .frame(width: 1, height: 16)
        }

        ForEach(summary.duplicateLines.prefix(4), id: \.fingerprint) { duplicate in
            Button {
                onSearchDuplicate(duplicate)
            } label: {
                Label("\(duplicate.count)x", systemImage: "square.stack.3d.up")
                    .labelStyle(.titleAndIcon)
                    .monospacedDigit()
                    .padding(.horizontal, RuneSpacing.xs)
            }
            .buttonStyle(RuneCompactButtonStyle())
            .help("Search repeated log line: \(duplicate.fingerprint)")
        }
    }

    private var summarySubtitle: String {
        if summary.isStructured {
            let percent = Int((summary.structuredRatio * 100).rounded())
            return "\(summary.jsonLineCount) of \(summary.totalLineCount) JSON lines, \(percent)% structured"
        }
        if let duplicate = summary.duplicateLines.first {
            return "Top duplicate appears \(duplicate.count) times"
        }
        return "\(summary.totalLineCount) lines"
    }

    private func fieldMenu(_ field: ResourceStructuredLogField) -> some View {
        RuneToolbarMenu {
            ForEach(field.sampleValues, id: \.self) { value in
                Button {
                    onSearchFieldSample(field, value)
                } label: {
                    Text(value)
                }
            }
        } label: {
            Label("\(field.title) \(field.nonEmptyCount)", systemImage: "number")
        }
        .help("Search structured field \(field.title)")
    }
}

struct ResourceLogsPaneActions {
    let onReload: () -> Void
    let onSave: () -> Void
    var onSaveToExportFolder: () -> Void = {}
    var onSaveAndOpen: () -> Void = {}
    let onSaveVisibleZip: (String) -> Void
    var onSaveVisibleZipToExportFolder: (String) -> Void = { _ in }
    var onSaveVisibleZipAndOpen: (String) -> Void = { _ in }
    let onSaveFullZip: () -> Void
    var onSaveFullZipToExportFolder: () -> Void = {}
    var onSaveFullZipAndOpen: () -> Void = {}
    let onSaveAllPodsZip: () -> Void
    var onSaveAllPodsZipToExportFolder: () -> Void = {}
    var onSaveAllPodsZipAndOpen: () -> Void = {}
    let onCopySelection: () -> Void
    let onCopyAll: () -> Void
    let onToggleStreamPause: () -> Void
}

private struct ResourceLogsPaneSourceAdapter {
    let selectedContainer: Binding<String>
    let podOptions: [PodSummary]
    let selectedPodID: Binding<String>?
    let isFavoritePod: (PodSummary) -> Bool
    let onToggleFavoritePod: (PodSummary) -> Void
    let showsContainerPicker: Bool
    let containerOptions: [String]
    let sourcePanelTitle: String?
    let sourcePanelValues: [String]
    let emptyMessage: String
    let noMatchesMessage: String

    static func pod(
        selectedContainer: Binding<String>,
        podOptions: [PodSummary],
        selectedPodID: Binding<String>?,
        isFavoritePod: @escaping (PodSummary) -> Bool,
        onToggleFavoritePod: @escaping (PodSummary) -> Void,
        showsContainerPicker: Bool,
        containerOptions: [String]
    ) -> Self {
        Self(
            selectedContainer: selectedContainer,
            podOptions: podOptions,
            selectedPodID: selectedPodID,
            isFavoritePod: isFavoritePod,
            onToggleFavoritePod: onToggleFavoritePod,
            showsContainerPicker: showsContainerPicker,
            containerOptions: containerOptions,
            sourcePanelTitle: nil,
            sourcePanelValues: [],
            emptyMessage: "The pod may be idle, or the current filter returned no lines.",
            noMatchesMessage: "No log lines matched the current search."
        )
    }

    static func unified(podNames: [String]) -> Self {
        Self(
            selectedContainer: .constant(""),
            podOptions: [],
            selectedPodID: nil,
            isFavoritePod: { _ in false },
            onToggleFavoritePod: { _ in },
            showsContainerPicker: false,
            containerOptions: [],
            sourcePanelTitle: "Pods",
            sourcePanelValues: podNames,
            emptyMessage: "No lines were returned for the selected pods and the current filter. Pods may be idle or produce no output for this time window.",
            noMatchesMessage: "No unified log lines matched the current search."
        )
    }
}

struct ResourceLogsExplorePanel: View {
    @Environment(\.runeDocumentFrameEmbedded) private var isEmbedded
    @Binding var searchQuery: String
    @Binding var searchMatchCase: Bool
    @Binding var selectedSearchMatchIndex: Int
    let searchFocusRequestID: Int
    let searchResult: ResourceLogSearchResult?
    let structuredSummary: ResourceStructuredLogSummary
    let showsInsights: Bool
    let presentationStyle: ResourceLogsPresentationStyle
    let placeholder: String
    let findHelp: String
    let matchCaseHelp: String
    let onSearchFieldSample: (ResourceStructuredLogField, String) -> Void
    let onSearchDuplicate: (ResourceDuplicateLogLine) -> Void
    var onSearchNavigate: () -> Void = {}

    var body: some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.inspectorControlRowSpacing) {
            ResourceLogsSearchBar(
                query: $searchQuery,
                matchCase: $searchMatchCase,
                selectedMatchIndex: $selectedSearchMatchIndex,
                focusRequestID: searchFocusRequestID,
                searchSummary: searchResult,
                placeholder: placeholder,
                findHelp: findHelp,
                matchCaseHelp: matchCaseHelp,
                onNavigate: onSearchNavigate
            )

            if showsInsights && (structuredSummary.isStructured || !structuredSummary.duplicateLines.isEmpty) {
                ResourceStructuredLogSummaryPanel(
                    summary: structuredSummary,
                    onSearchFieldSample: onSearchFieldSample,
                    onSearchDuplicate: onSearchDuplicate,
                    presentationStyle: presentationStyle
                )
            }
        }
        .modifier(ResourceLogsExploreChrome(isEmbedded: isEmbedded))
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Log search and insights")
    }
}

/// Inside the log document's header bar the search sits directly in the bar.
private struct ResourceLogsExploreChrome: ViewModifier {
    let isEmbedded: Bool

    func body(content: Content) -> some View {
        if isEmbedded {
            content
                .frame(maxWidth: .infinity, alignment: .leading)
        } else {
            content
                .padding(.horizontal, RuneUILayoutMetrics.inspectorControlContentInset)
                .padding(.vertical, RuneUILayoutMetrics.inspectorControlSurfaceVerticalPadding)
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(RuneSurfaceBackground(kind: .inset))
        }
    }
}

actor ResourceLogsLatestWorkLane<Output: Sendable> {
    private var generation: UInt64 = 0
    private var currentTask: Task<Output, Error>?

    func run(
        priority: TaskPriority,
        operation: @escaping @Sendable () throws -> Output
    ) async throws -> Output {
        generation &+= 1
        let requestedGeneration = generation

        if let currentTask {
            currentTask.cancel()
            _ = await currentTask.result
        }

        try Task.checkCancellation()
        guard requestedGeneration == generation else { throw CancellationError() }

        let workTask = Task.detached(priority: priority) {
            try Task.checkCancellation()
            let output = try operation()
            try Task.checkCancellation()
            return output
        }
        currentTask = workTask

        let output = try await withTaskCancellationHandler {
            try await workTask.value
        } onCancel: {
            workTask.cancel()
        }

        try Task.checkCancellation()
        guard requestedGeneration == generation else { throw CancellationError() }
        currentTask = nil
        return output
    }

    func cancel() {
        generation &+= 1
        currentTask?.cancel()
    }
}

private struct ResourceLogsInspectorPaneCore: View {
    @Binding var selectedLogPreset: PodLogPreset
    @Binding var includePreviousLogs: Bool
    @Binding var isTailModeEnabled: Bool
    @Binding var isStreamPaused: Bool
    let isLoadingLogs: Bool
    let isLoadingResources: Bool
    let errorMessage: String?
    let statusText: String
    let source: ResourceLogsPaneSourceAdapter
    let presentationStyle: ResourceLogsPresentationStyle
    let logText: String
    let readOnlyResetID: String
    let actions: ResourceLogsPaneActions
    @AppStorage(RuneSettingsKeys.simpleMode) private var simpleMode = false
    @AppStorage(RuneSettingsKeys.interfaceLanguage) private var interfaceLanguageRaw =
        RuneSettingsKeys.interfaceLanguageDefault
    @Environment(\.runeSurfaceTabs) private var surfaceTabs
    /// Insights (structured fields, repeated lines) sit in a slim line at the
    /// bottom of the log; the reader can dismiss it for this view.
    @State private var hidesInsightsLine = false
    @State private var searchQuery = ""
    @State private var searchMatchCase = false
    @State private var selectedSearchMatchIndex = 0
    @State private var searchNavigationSequence = 0
    @State private var searchFocusRequestID = 0
    @State private var searchResult = ResourceLogSearchResult.make(text: "", query: "")
    @State private var searchTask: Task<Void, Never>?
    @State private var searchLifecycleGeneration = 0
    @State private var searchWorkLane =
        RuneSingleFlightLatestPendingWorkLane<ResourceLogSearchResult>()
    @State private var isInitialSearchNavigationPending = false
    @State private var structuredLogSummary = ResourceStructuredLogAnalyzer.analyze(text: "")
    @State private var structuredSummaryTask: Task<Void, Never>?
    @State private var structuredSummaryWorkLane = ResourceLogsLatestWorkLane<ResourceStructuredLogSummary>()

    var body: some View {
        let activeSearchResult = searchResult.isSearchNavigableSnapshot(
            for: logText,
            query: searchQuery,
            matchCase: searchMatchCase
        ) ? searchResult : nil
        let renderSearchResult = searchResult.isRenderableSnapshot(for: logText)
            ? searchResult
            : nil
        let resolvedSearchMatchIndex = activeSearchResult?.clampedMatchIndex(selectedSearchMatchIndex) ?? 0

        // One framed log document: source, actions and search in a bar
        // attached to its top edge, any tabs that switch the log on that
        // edge, the output below. No cards inside.
        // One framed log document (Design/README.md, "Log viewer"): a
        // two-row header attached to its top edge (source and range; follow,
        // reload, search, save, more), any tabs that switch the log on that
        // edge, the output, and insights as a slim line at the bottom.
        RuneDocumentFrame(
            showsTabs: surfaceTabs != nil,
            showsStatus: showsInsightsLine
        ) {
            ResourceLogsToolbar(
                selectedLogPreset: $selectedLogPreset,
                includePreviousLogs: $includePreviousLogs,
                selectedContainer: source.selectedContainer,
                isTailModeEnabled: $isTailModeEnabled,
                isStreamPaused: $isStreamPaused,
                statusText: statusText,
                podOptions: source.podOptions,
                selectedPodID: source.selectedPodID,
                isFavoritePod: source.isFavoritePod,
                onToggleFavoritePod: source.onToggleFavoritePod,
                presentationStyle: presentationStyle,
                showsContainerPicker: source.showsContainerPicker,
                containerOptions: source.containerOptions,
                sourceSummaryTitle: source.sourcePanelTitle,
                sourceSummaryValues: source.sourcePanelValues,
                visibleLogText: logText,
                onReload: actions.onReload,
                onSave: actions.onSave,
                onSaveToExportFolder: actions.onSaveToExportFolder,
                onSaveAndOpen: actions.onSaveAndOpen,
                onSaveVisibleZip: actions.onSaveVisibleZip,
                onSaveVisibleZipToExportFolder: actions.onSaveVisibleZipToExportFolder,
                onSaveVisibleZipAndOpen: actions.onSaveVisibleZipAndOpen,
                onSaveFullZip: actions.onSaveFullZip,
                onSaveFullZipToExportFolder: actions.onSaveFullZipToExportFolder,
                onSaveFullZipAndOpen: actions.onSaveFullZipAndOpen,
                onSaveAllPodsZip: actions.onSaveAllPodsZip,
                onSaveAllPodsZipToExportFolder: actions.onSaveAllPodsZipToExportFolder,
                onSaveAllPodsZipAndOpen: actions.onSaveAllPodsZipAndOpen,
                onCopySelection: actions.onCopySelection,
                onCopyAll: actions.onCopyAll,
                onToggleStreamPause: actions.onToggleStreamPause,
                interfaceLanguageRaw: interfaceLanguageRaw,
                searchField: AnyView(logSearchBar(activeSearchResult: activeSearchResult))
            )
            .id(interfaceLanguageRaw)
            .frame(maxWidth: .infinity, alignment: .leading)
        } tabs: {
            if let surfaceTabs {
                surfaceTabs
            }
        } content: {
            ResourceLogsOutputSurface(
                isLoadingLogs: isLoadingLogs,
                isLoadingResources: isLoadingResources,
                errorMessage: errorMessage,
                logText: logText,
                renderSearchResult: renderSearchResult,
                navigationSearchResult: activeSearchResult,
                selectedSearchMatchIndex: resolvedSearchMatchIndex,
                searchNavigationSequence: searchNavigationSequence,
                emptyTitle: "No log output",
                emptyMessage: source.emptyMessage,
                noMatchesMessage: source.noMatchesMessage,
                readOnlyResetID: readOnlyResetID,
                onReload: actions.onReload,
                presentationStyle: presentationStyle
            )
            .frame(maxWidth: .infinity, alignment: .leading)
        } status: {
            HStack(alignment: .firstTextBaseline, spacing: RuneSpacing.xs) {
                ResourceStructuredLogSummaryPanel(
                    summary: structuredLogSummary,
                    onSearchFieldSample: { field, value in
                        applyLogSearchQuery(ResourceStructuredLogFieldSearch.query(field: field, value: value))
                    },
                    onSearchDuplicate: { duplicate in
                        applyLogSearchQuery(duplicate.fingerprint)
                    },
                    presentationStyle: .terminalCompact
                )
                .frame(maxWidth: .infinity, alignment: .leading)
                RuneIconButton("Hide log insights", systemImage: "xmark") {
                    hidesInsightsLine = true
                }
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .onChange(of: searchQuery) { _, _ in
            selectedSearchMatchIndex = 0
            isInitialSearchNavigationPending =
                !searchQuery.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            scheduleSearch(
                for: logText,
                debounceNanoseconds: ResourceLogsLayoutMetrics.querySearchDebounceNanoseconds
            )
        }
        .onChange(of: searchMatchCase) { _, _ in
            selectedSearchMatchIndex = 0
            isInitialSearchNavigationPending =
                !searchQuery.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            scheduleSearch(
                for: logText,
                debounceNanoseconds: ResourceLogsLayoutMetrics.querySearchDebounceNanoseconds
            )
        }
        .onChange(of: logText) { _, newText in
            scheduleSearch(
                for: newText,
                debounceNanoseconds: ResourceLogsLayoutMetrics.streamedTextSearchDebounceNanoseconds
            )
            scheduleStructuredSummary(for: newText, debounced: true)
        }
        .onChange(of: simpleMode) { _, _ in
            scheduleStructuredSummary(for: logText, debounced: false)
        }
        .onAppear {
            isInitialSearchNavigationPending =
                !searchQuery.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            scheduleSearch(for: logText, debounceNanoseconds: 0)
            scheduleStructuredSummary(for: logText, debounced: false)
        }
        .onDisappear {
            searchLifecycleGeneration &+= 1
            searchTask?.cancel()
            structuredSummaryTask?.cancel()
            Task {
                await searchWorkLane.cancel()
                await structuredSummaryWorkLane.cancel()
            }
        }
    }

    private var showsInsightsLine: Bool {
        !simpleMode && !hidesInsightsLine
            && (structuredLogSummary.isStructured || !structuredLogSummary.duplicateLines.isEmpty)
    }

    private func logSearchBar(activeSearchResult: ResourceLogSearchResult?) -> some View {
        ResourceLogsSearchBar(
            query: $searchQuery,
            matchCase: $searchMatchCase,
            selectedMatchIndex: $selectedSearchMatchIndex,
            focusRequestID: searchFocusRequestID,
            searchSummary: activeSearchResult,
            placeholder: t(.searchLogs),
            findHelp: t(.findInLogs),
            matchCaseHelp: t(.matchCase),
            onNavigate: {
                searchNavigationSequence &+= 1
            }
        )
    }

    private func scheduleStructuredSummary(for text: String, debounced: Bool) {
        structuredSummaryTask?.cancel()
        guard !simpleMode else {
            structuredLogSummary = ResourceStructuredLogAnalyzer.analyze(text: "")
            return
        }

        structuredSummaryTask = Task { @MainActor in
            if debounced {
                try? await Task.sleep(
                    nanoseconds: ResourceLogsLayoutMetrics.streamedTextSummaryDebounceNanoseconds
                )
            }
            guard !Task.isCancelled else { return }

            guard let summary = try? await structuredSummaryWorkLane.run(
                priority: .utility,
                operation: {
                    try ResourceStructuredLogAnalyzer.analyze(
                        text: text,
                        cancellationCheck: { try Task.checkCancellation() }
                    )
                }
            ) else { return }
            guard !Task.isCancelled, !simpleMode, logText == text else { return }
            structuredLogSummary = summary
        }
    }

    private func scheduleSearch(for text: String, debounceNanoseconds: UInt64) {
        searchTask?.cancel()
        let requestedQuery = searchQuery
        let requestedMatchCase = searchMatchCase
        let lifecycleGeneration = searchLifecycleGeneration
        let reusableTextIndex = searchResult.originalText == text ? searchResult.textIndex : nil
        let task = Task { @MainActor in
            if debounceNanoseconds > 0 {
                try? await Task.sleep(nanoseconds: debounceNanoseconds)
            }
            guard !Task.isCancelled,
                  searchLifecycleGeneration == lifecycleGeneration else {
                return
            }
            if debounceNanoseconds > 0 {
                searchTask = nil
            }

            guard let result = await searchWorkLane.submit(
                priority: .userInitiated,
                operation: {
                    try ResourceLogSearchResult.makeForInspector(
                        text: text,
                        textIndex: reusableTextIndex,
                        query: requestedQuery,
                        matchCase: requestedMatchCase,
                        cancellationCheck: { try Task.checkCancellation() }
                    )
                }
            ) else { return }

            guard searchLifecycleGeneration == lifecycleGeneration,
                  searchQuery == requestedQuery,
                  searchMatchCase == requestedMatchCase else {
                return
            }
            searchResult = result
            selectedSearchMatchIndex = result.clampedMatchIndex(selectedSearchMatchIndex)
            if isInitialSearchNavigationPending {
                isInitialSearchNavigationPending = false
                searchNavigationSequence &+= 1
            }
        }
        if debounceNanoseconds > 0 {
            searchTask = task
        }
    }

    private func applyLogSearchQuery(_ query: String) {
        searchQuery = query
        searchFocusRequestID += 1
    }

    private var language: RuneLanguage {
        RuneLanguage.resolved(interfaceLanguageRaw)
    }

    private func t(_ key: RuneLocalizedStringKey) -> String {
        RuneLocalizedStrings.shared.string(key, language: language)
    }
}

struct PodLogsInspectorPane: View {
    @Binding var selectedLogPreset: PodLogPreset
    @Binding var includePreviousLogs: Bool
    @Binding var selectedContainer: String
    @Binding var isTailModeEnabled: Bool
    @Binding var isStreamPaused: Bool
    let isLoadingLogs: Bool
    let isLoadingResources: Bool
    let errorMessage: String?
    let statusText: String
    var podOptions: [PodSummary] = []
    var selectedPodID: Binding<String>? = nil
    var isFavoritePod: (PodSummary) -> Bool = { _ in false }
    var onToggleFavoritePod: (PodSummary) -> Void = { _ in }
    var presentationStyle: ResourceLogsPresentationStyle = .regular
    var showsContainerPicker: Bool = true
    let containerOptions: [String]
    let logText: String
    let readOnlyResetID: String
    let onReload: () -> Void
    let onSave: () -> Void
    var onSaveToExportFolder: () -> Void = {}
    var onSaveAndOpen: () -> Void = {}
    let onSaveVisibleZip: (String) -> Void
    var onSaveVisibleZipToExportFolder: (String) -> Void = { _ in }
    var onSaveVisibleZipAndOpen: (String) -> Void = { _ in }
    let onSaveFullZip: () -> Void
    var onSaveFullZipToExportFolder: () -> Void = {}
    var onSaveFullZipAndOpen: () -> Void = {}
    let onSaveAllPodsZip: () -> Void
    var onSaveAllPodsZipToExportFolder: () -> Void = {}
    var onSaveAllPodsZipAndOpen: () -> Void = {}
    let onCopySelection: () -> Void
    let onCopyAll: () -> Void
    let onToggleStreamPause: () -> Void

    var body: some View {
        ResourceLogsInspectorPaneCore(
            selectedLogPreset: $selectedLogPreset,
            includePreviousLogs: $includePreviousLogs,
            isTailModeEnabled: $isTailModeEnabled,
            isStreamPaused: $isStreamPaused,
            isLoadingLogs: isLoadingLogs,
            isLoadingResources: isLoadingResources,
            errorMessage: errorMessage,
            statusText: statusText,
            source: .pod(
                selectedContainer: $selectedContainer,
                podOptions: podOptions,
                selectedPodID: selectedPodID,
                isFavoritePod: isFavoritePod,
                onToggleFavoritePod: onToggleFavoritePod,
                showsContainerPicker: showsContainerPicker,
                containerOptions: containerOptions
            ),
            presentationStyle: presentationStyle,
            logText: logText,
            readOnlyResetID: readOnlyResetID,
            actions: actions
        )
    }

    private var actions: ResourceLogsPaneActions {
        ResourceLogsPaneActions(
            onReload: onReload,
            onSave: onSave,
            onSaveToExportFolder: onSaveToExportFolder,
            onSaveAndOpen: onSaveAndOpen,
            onSaveVisibleZip: onSaveVisibleZip,
            onSaveVisibleZipToExportFolder: onSaveVisibleZipToExportFolder,
            onSaveVisibleZipAndOpen: onSaveVisibleZipAndOpen,
            onSaveFullZip: onSaveFullZip,
            onSaveFullZipToExportFolder: onSaveFullZipToExportFolder,
            onSaveFullZipAndOpen: onSaveFullZipAndOpen,
            onSaveAllPodsZip: onSaveAllPodsZip,
            onSaveAllPodsZipToExportFolder: onSaveAllPodsZipToExportFolder,
            onSaveAllPodsZipAndOpen: onSaveAllPodsZipAndOpen,
            onCopySelection: onCopySelection,
            onCopyAll: onCopyAll,
            onToggleStreamPause: onToggleStreamPause
        )
    }
}

struct UnifiedResourceLogsInspectorPane: View {
    @Binding var selectedLogPreset: PodLogPreset
    @Binding var includePreviousLogs: Bool
    @Binding var isTailModeEnabled: Bool
    @Binding var isStreamPaused: Bool
    let isLoadingLogs: Bool
    let isLoadingResources: Bool
    let errorMessage: String?
    let statusText: String
    let podNames: [String]
    let logText: String
    let readOnlyResetID: String
    let onReload: () -> Void
    let onSave: () -> Void
    var onSaveToExportFolder: () -> Void = {}
    var onSaveAndOpen: () -> Void = {}
    let onSaveVisibleZip: (String) -> Void
    var onSaveVisibleZipToExportFolder: (String) -> Void = { _ in }
    var onSaveVisibleZipAndOpen: (String) -> Void = { _ in }
    let onSaveFullZip: () -> Void
    var onSaveFullZipToExportFolder: () -> Void = {}
    var onSaveFullZipAndOpen: () -> Void = {}
    let onSaveAllPodsZip: () -> Void
    var onSaveAllPodsZipToExportFolder: () -> Void = {}
    var onSaveAllPodsZipAndOpen: () -> Void = {}
    let onCopySelection: () -> Void
    let onCopyAll: () -> Void
    let onToggleStreamPause: () -> Void

    var body: some View {
        ResourceLogsInspectorPaneCore(
            selectedLogPreset: $selectedLogPreset,
            includePreviousLogs: $includePreviousLogs,
            isTailModeEnabled: $isTailModeEnabled,
            isStreamPaused: $isStreamPaused,
            isLoadingLogs: isLoadingLogs,
            isLoadingResources: isLoadingResources,
            errorMessage: errorMessage,
            statusText: statusText,
            source: .unified(podNames: podNames),
            presentationStyle: .regular,
            logText: logText,
            readOnlyResetID: readOnlyResetID,
            actions: actions
        )
    }

    private var actions: ResourceLogsPaneActions {
        ResourceLogsPaneActions(
            onReload: onReload,
            onSave: onSave,
            onSaveToExportFolder: onSaveToExportFolder,
            onSaveAndOpen: onSaveAndOpen,
            onSaveVisibleZip: onSaveVisibleZip,
            onSaveVisibleZipToExportFolder: onSaveVisibleZipToExportFolder,
            onSaveVisibleZipAndOpen: onSaveVisibleZipAndOpen,
            onSaveFullZip: onSaveFullZip,
            onSaveFullZipToExportFolder: onSaveFullZipToExportFolder,
            onSaveFullZipAndOpen: onSaveFullZipAndOpen,
            onSaveAllPodsZip: onSaveAllPodsZip,
            onSaveAllPodsZipToExportFolder: onSaveAllPodsZipToExportFolder,
            onSaveAllPodsZipAndOpen: onSaveAllPodsZipAndOpen,
            onCopySelection: onCopySelection,
            onCopyAll: onCopyAll,
            onToggleStreamPause: onToggleStreamPause
        )
    }
}

struct ResourceLogsOutputSurface: View {
    let isLoadingLogs: Bool
    let isLoadingResources: Bool
    let errorMessage: String?
    let logText: String
    let renderSearchResult: ResourceLogSearchResult?
    let navigationSearchResult: ResourceLogSearchResult?
    let selectedSearchMatchIndex: Int
    let searchNavigationSequence: Int
    let emptyTitle: String
    let emptyMessage: String
    let noMatchesMessage: String
    let readOnlyResetID: String
    let onReload: () -> Void
    let presentationStyle: ResourceLogsPresentationStyle

    var body: some View {
        InspectorTextSurface(
            minHeight: ResourceLogsLayoutMetrics.outputMinimumHeight(for: presentationStyle)
        ) {
            Group {
                if isLoadingLogs || isLoadingResources {
                    ResourceLogsLoadingPlaceholder()
                } else if let errorMessage {
                    ResourceLogsErrorView(message: errorMessage, onReload: onReload)
                } else if logText.allSatisfy(\.isWhitespace) {
                    ResourceLogsEmptyPlaceholder(title: emptyTitle, message: emptyMessage)
                } else {
                    let outputResetID = "\(readOnlyResetID):logs"
                    let usesLargeTextSurface =
                        ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: logText)
                    InspectorReadOnlyTextView(
                        text: logText,
                        resetID: outputResetID,
                        resetScrollOnExternalChange: false,
                        contentStyle: .ansiLogs,
                        usesLargeTextSurface: usesLargeTextSurface,
                        allowsAutomaticLargeTextSurface: false,
                        largeTextIndex: renderSearchResult?.textIndex,
                        largeTextScrollTargetLine: navigationSearchResult?.matchLineNumber(selectedIndex: selectedSearchMatchIndex),
                        largeTextScrollTargetRevision: navigationSearchResult?.largeTextNavigationRevision(
                            selectedIndex: selectedSearchMatchIndex,
                            sequence: searchNavigationSequence
                        ),
                        largeTextScrollsOnTargetLineChange: false,
                        largeTextShowsLineNumbers: false,
                        searchQuery: navigationSearchResult?.query ?? "",
                        searchMatchCase: navigationSearchResult?.matchCase ?? false,
                        selectedSearchMatchIndex: selectedSearchMatchIndex,
                        searchMatchRanges: navigationSearchResult?.matchRanges ?? [],
                        searchNavigationRevision: searchNavigationSequence
                    )
                }
            }
        }
        .layoutPriority(presentationStyle == .terminalCompact ? 1 : 0)
    }
}

enum ResourceLogsDeferredRenderingPolicy {
    static let deferredOutputThreshold = 250_000

    static func shouldDeferOutputMount(for result: ResourceLogSearchResult) -> Bool {
        result.displayedText.utf8.count > deferredOutputThreshold
    }

    /// Matches the indexed policy while a fresh index is pending, including payloads
    /// made up of relatively few very wide lines.
    static func shouldDeferOutputMount(for text: String) -> Bool {
        text.utf8.count > deferredOutputThreshold
    }
}

struct ResourceLogsSearchBar: View {
    @Binding var query: String
    @Binding var matchCase: Bool
    @Binding var selectedMatchIndex: Int
    let focusRequestID: Int
    let searchSummary: ResourceLogSearchResult?
    let placeholder: String
    let findHelp: String
    let matchCaseHelp: String
    var onNavigate: () -> Void = {}
    @State private var isJumpPopoverPresented = false
    @State private var jumpText = ""
    @State private var jumpMatchCount = 0
    @FocusState private var isSearchFocused: Bool
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        RuneInspectorControlGridRow {
            RuneIconButton(findHelp, systemImage: "magnifyingglass") {
                isSearchFocused = true
            }
            .keyboardShortcut("f", modifiers: [.command])
        } content: {
            HStack(spacing: RuneSpacing.hairline) {
                TextField(
                    "",
                    text: $query,
                    prompt: Text(placeholder)
                        .foregroundStyle(runeThemePalette?.mutedText ?? Color.secondary)
                )
                .textFieldStyle(.plain)
                .runeInterfaceFont(relativeSize: RuneControlSize.large.labelRelativeSize)
                .foregroundStyle(runeThemePalette?.foreground ?? Color.primary)
                .tint(runeThemePalette?.accent ?? Color.accentColor)
                .focused($isSearchFocused)
                .frame(
                    minWidth: ResourceLogsLayoutMetrics.searchFieldMinimumWidth,
                    idealWidth: ResourceLogsLayoutMetrics.searchFieldIdealWidth,
                    maxWidth: .infinity
                )
                .layoutPriority(1)
                .accessibilityLabel(placeholder)
                .accessibilityIdentifier("resource-log-search-input")
                .onSubmit {
                    selectNextMatch()
                }
                .runeTextInputCursor()

                ViewThatFits(in: .horizontal) {
                    searchAccessories(compact: false)
                    searchAccessories(compact: true)
                }
                .layoutPriority(2)
            }
        }
        .frame(
            minWidth: ResourceLogsLayoutMetrics.searchChromeMinimumWidth,
            idealWidth: ResourceLogsLayoutMetrics.searchChromeIdealWidth,
            maxWidth: .infinity,
            minHeight: ResourceLogsLayoutMetrics.searchChromeHeight,
            maxHeight: ResourceLogsLayoutMetrics.searchChromeHeight,
            alignment: .leading
        )
        .background(RuneSurfaceBackground(kind: .editor))
        .runePointerCursor()
        .onChange(of: focusRequestID) { _, _ in
            isSearchFocused = true
        }
        .onChange(of: query) { _, _ in
            isJumpPopoverPresented = false
        }
        .onChange(of: matchCase) { _, _ in
            isJumpPopoverPresented = false
        }
        .transaction { transaction in
            transaction.animation = nil
        }
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Log find controls")
        .accessibilityIdentifier("resource-log-search-chrome")
    }

    private func searchAccessories(compact: Bool) -> some View {
        ResourceLogSearchAccessories(
            compact: compact,
            hasQuery: !normalizedQuery.isEmpty,
            matchCase: matchCase,
            hasMatches: searchSummary?.hasMatches == true,
            status: matchStatusText,
            statusLabel: matchStatusAccessibilityLabel,
            statusColor: matchStatusColor,
            matchCaseHelp: matchCaseHelp
        ) { action in
            switch action {
            case .clear: query = ""
            case .matchCase: matchCase.toggle()
            case .previous: selectPreviousMatch()
            case .next: selectNextMatch()
            case .jump:
                if let searchSummary { prepareJumpPopover(for: searchSummary) }
            }
        }
        .frame(width: compact ? ResourceLogSearchAccessories.compactWidth : ResourceLogSearchAccessories.regularWidth,
               height: RuneUILayoutMetrics.iconButtonSize)
        .popover(isPresented: $isJumpPopoverPresented, arrowEdge: .bottom) {
            if jumpMatchCount > 0 { jumpToMatchPopover(matchCount: jumpMatchCount) }
        }
    }

    private func selectPreviousMatch() {
        guard let searchSummary else { return }
        selectedMatchIndex = searchSummary.previousMatchIndex(from: selectedMatchIndex)
        onNavigate()
    }

    private func selectNextMatch() {
        guard let searchSummary else { return }
        selectedMatchIndex = searchSummary.nextMatchIndex(from: selectedMatchIndex)
        onNavigate()
    }

    private var normalizedQuery: String {
        query.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var matchStatusText: String {
        guard !normalizedQuery.isEmpty else { return "\u{00a0}" }
        guard let searchSummary else { return "…" }
        guard searchSummary.hasMatches else { return "No results" }
        return searchSummary.matchPositionText(selectedIndex: selectedMatchIndex)
    }

    private var matchStatusColor: Color {
        guard !normalizedQuery.isEmpty else { return .clear }
        guard let searchSummary else {
            return runeThemePalette?.mutedText ?? Color.secondary.opacity(0.72)
        }
        if !searchSummary.hasMatches {
            return RuneSemanticColorRole.danger.color(in: runeThemePalette)
        }
        return runeThemePalette?.secondaryText ?? Color.secondary
    }

    private var matchStatusAccessibilityLabel: String {
        guard !normalizedQuery.isEmpty else { return "Search match position" }
        guard let searchSummary else { return "Searching logs" }
        guard searchSummary.hasMatches else { return "No search results" }
        return "Search match \(searchSummary.matchPositionText(selectedIndex: selectedMatchIndex))"
    }

    private func prepareJumpPopover(for searchSummary: ResourceLogSearchResult) {
        guard searchSummary.hasMatches else { return }
        jumpText = "\(searchSummary.clampedMatchIndex(selectedMatchIndex) + 1)"
        jumpMatchCount = searchSummary.matchRanges.count
        isJumpPopoverPresented = true
    }

    private func jumpToMatchPopover(matchCount: Int) -> some View {
        VStack(alignment: .leading, spacing: RuneSpacing.inset) {
            Text("Go to match")
                .runeText(.headline)

            HStack(spacing: RuneSpacing.xs) {
                TextField("Match", text: $jumpText)
                    .textFieldStyle(RuneControlTextFieldStyle())
                    .fontDesign(.monospaced)
                    .frame(width: 82)
                    .onSubmit {
                        commitJump(matchCount: matchCount)
                    }
                    .runeTextInputCursor()

                Text("of \(matchCount)")
                    .runeText(.micro, weight: .medium)
                    .foregroundStyle(.runeSecondary)
                    .monospacedDigit()
            }

            HStack {
                Spacer()
                Button("Go") {
                    commitJump(matchCount: matchCount)
                }
                .keyboardShortcut(.defaultAction)
            }
        }
        .padding(RuneSpacing.sm)
        .frame(width: 180)
        .runePointerCursor()
    }

    private func commitJump(matchCount: Int) {
        guard matchCount > 0 else { return }
        let requestedMatch = Int(jumpText.trimmingCharacters(in: .whitespacesAndNewlines)) ?? 1
        selectedMatchIndex = min(max(requestedMatch, 1), matchCount) - 1
        onNavigate()
        isJumpPopoverPresented = false
    }

}

struct ResourceLogSearchResult: Equatable, Sendable {
    let originalText: String
    let displayedText: String
    let query: String
    let matchCase: Bool
    let totalLineCount: Int
    let matchingLineCount: Int
    let matchRanges: [NSRange]
    let textIndex: RuneLargeTextIndex

    var matchLineNumbers: [Int] {
        matchRanges.map { textIndex.lineNumber(containingUTF16Location: $0.location) }
    }

    var isFiltering: Bool {
        !query.isEmpty
    }

    var scrollIdentityToken: String {
        "logs"
    }

    var badgeText: String {
        matchingLineCount == 1 ? "1 match" : "\(matchingLineCount) matches"
    }

    var hasMatches: Bool {
        !matchRanges.isEmpty
    }

    func isCurrent(text: String, query: String, matchCase: Bool) -> Bool {
        originalText == text
            && self.query == query.trimmingCharacters(in: .whitespacesAndNewlines)
            && self.matchCase == matchCase
    }

    func isRenderableSnapshot(for text: String) -> Bool {
        originalText == text
            || (!originalText.isEmpty && text.hasPrefix(originalText))
    }

    func isSearchNavigableSnapshot(
        for text: String,
        query: String,
        matchCase: Bool
    ) -> Bool {
        isRenderableSnapshot(for: text)
            && self.query == query.trimmingCharacters(in: .whitespacesAndNewlines)
            && self.matchCase == matchCase
    }

    func matchPositionText(selectedIndex: Int) -> String {
        guard hasMatches else { return "0 of 0" }
        return "\(clampedMatchIndex(selectedIndex) + 1) of \(matchRanges.count)"
    }

    var summaryText: String {
        if matchingLineCount == 0 {
            return "No matches in \(totalLineCount) total lines."
        }
        return "\(matchingLineCount) matches in \(totalLineCount) total lines."
    }

    static func make(text: String, query: String) -> ResourceLogSearchResult {
        make(text: text, query: query, matchCase: false)
    }

    static func make(text: String, query: String, matchCase: Bool) -> ResourceLogSearchResult {
        let textIndex = RuneLargeTextIndex(text: text)
        return make(textIndex: textIndex, query: query, matchCase: matchCase)
    }

    static func make(
        textIndex: RuneLargeTextIndex,
        query: String,
        matchCase: Bool
    ) -> ResourceLogSearchResult {
        make(
            textIndex: textIndex,
            query: query,
            matchCase: matchCase,
            cancellationCheck: {}
        )
    }

    static func make(
        textIndex: RuneLargeTextIndex,
        query: String,
        matchCase: Bool,
        cancellationCheck: () throws -> Void
    ) rethrows -> ResourceLogSearchResult {
        try make(
            originalText: textIndex.text,
            textIndex: textIndex,
            query: query,
            matchCase: matchCase,
            cancellationCheck: cancellationCheck
        )
    }

    private static func make(
        originalText: String,
        textIndex: RuneLargeTextIndex,
        query: String,
        matchCase: Bool,
        cancellationCheck: () throws -> Void
    ) rethrows -> ResourceLogSearchResult {
        try cancellationCheck()
        let displayedText = textIndex.text
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)

        guard !trimmedQuery.isEmpty else {
            return ResourceLogSearchResult(
                originalText: originalText,
                displayedText: displayedText,
                query: "",
                matchCase: matchCase,
                totalLineCount: textIndex.lineCount,
                matchingLineCount: textIndex.lineCount,
                matchRanges: [],
                textIndex: textIndex
            )
        }

        let options: NSString.CompareOptions = matchCase ? [] : [.caseInsensitive, .diacriticInsensitive]
        let matchRanges = try textIndex.searchRanges(
            query: trimmedQuery,
            options: options,
            cancellationCheck: cancellationCheck
        )
        try cancellationCheck()

        return ResourceLogSearchResult(
            originalText: originalText,
            displayedText: displayedText,
            query: trimmedQuery,
            matchCase: matchCase,
            totalLineCount: textIndex.lineCount,
            matchingLineCount: matchRanges.count,
            matchRanges: matchRanges,
            textIndex: textIndex
        )
    }

    func clampedMatchIndex(_ index: Int) -> Int {
        guard !matchRanges.isEmpty else { return 0 }
        return min(max(index, 0), matchRanges.count - 1)
    }

    func nextMatchIndex(from index: Int) -> Int {
        guard !matchRanges.isEmpty else { return 0 }
        return (clampedMatchIndex(index) + 1) % matchRanges.count
    }

    func previousMatchIndex(from index: Int) -> Int {
        guard !matchRanges.isEmpty else { return 0 }
        return (clampedMatchIndex(index) - 1 + matchRanges.count) % matchRanges.count
    }

    func matchLineNumber(selectedIndex: Int) -> Int? {
        guard !matchRanges.isEmpty else { return nil }
        let range = matchRanges[clampedMatchIndex(selectedIndex)]
        return textIndex.lineNumber(containingUTF16Location: range.location)
    }

    func navigationRequest(selectedIndex: Int, sequence: Int) -> YAMLTextNavigationRequest? {
        guard !matchRanges.isEmpty else { return nil }
        let range = matchRanges[clampedMatchIndex(selectedIndex)]
        return YAMLTextNavigationRequest(
            issueID: "resource-log-search:\(query):\(selectedIndex)",
            sequence: sequence,
            range: YAMLValidationRange(location: range.location, length: range.length),
            line: nil,
            column: nil
        )
    }

    func largeTextNavigationRevision(selectedIndex: Int, sequence: Int) -> Int? {
        return sequence &* 1_000_003 &+ clampedMatchIndex(selectedIndex)
    }

    static func makeForInspector(
        text: String,
        textIndex: RuneLargeTextIndex? = nil,
        query: String,
        matchCase: Bool = false
    ) -> ResourceLogSearchResult {
        makeForInspector(
            text: text,
            textIndex: textIndex,
            query: query,
            matchCase: matchCase,
            cancellationCheck: {}
        )
    }

    static func makeForInspector(
        text: String,
        textIndex: RuneLargeTextIndex? = nil,
        query: String,
        matchCase: Bool = false,
        cancellationCheck: () throws -> Void
    ) rethrows -> ResourceLogSearchResult {
        try cancellationCheck()
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        if let textIndex, textIndex.text == text {
            return try make(
                originalText: text,
                textIndex: textIndex,
                query: trimmedQuery,
                matchCase: matchCase,
                cancellationCheck: cancellationCheck
            )
        }
        let displayedText = ResourceLogANSIFormatter.plainText(from: text)
        if let textIndex, textIndex.text == displayedText {
            return try make(
                originalText: text,
                textIndex: textIndex,
                query: trimmedQuery,
                matchCase: matchCase,
                cancellationCheck: cancellationCheck
            )
        }
        let textIndex = try RuneLargeTextIndex(
            text: displayedText,
            cancellationCheck: cancellationCheck
        )
        return try make(
            originalText: text,
            textIndex: textIndex,
            query: trimmedQuery,
            matchCase: matchCase,
            cancellationCheck: cancellationCheck
        )
    }
}

private struct ResourceLogsLoadingPlaceholder: View {
    var body: some View {
        RuneContentStateView(
            .loading(
                title: "Loading logs…",
                message: "Fetching log output for the current selection."
            ),
            variant: .centered
        )
    }
}

private struct ResourceLogsErrorView: View {
    let message: String
    let onReload: () -> Void

    var body: some View {
        RuneContentStateView(
            .retryableError(title: "Could not load logs", message: message),
            variant: .centered,
            action: RuneContentStateAction(
                "Retry",
                systemImage: "arrow.clockwise",
                perform: onReload
            )
        )
    }
}

private struct ResourceLogsEmptyPlaceholder: View {
    let title: String
    let message: String

    var body: some View {
        RuneContentStateView(
            .empty(title: title, message: message),
            variant: .centered
        )
    }
}
