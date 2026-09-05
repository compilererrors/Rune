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
    static let searchChromeHeight: CGFloat = RuneUILayoutMetrics.inspectorControlRowHeight
    static let searchChromeMinimumWidth: CGFloat = 220
    static let searchChromeIdealWidth: CGFloat = 420
    static let searchChromeMaximumWidth: CGFloat = 460
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
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @Environment(\.runeInterfaceFontSize) private var interfaceFontSize

    var body: some View {
        switch presentationStyle {
        case .regular:
            regularBody
        case .terminalCompact:
            terminalCompactBody
        }
    }

    private var regularBody: some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.inspectorControlRowSpacing) {
            LogToolbarScrollRow {
                primaryControls
            }
            .controlSize(toolbarControlSize)

            LogToolbarScrollRow {
                toolbarActions
            }
            .controlSize(toolbarControlSize)
        }
    }

    private var terminalCompactBody: some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.inspectorCompactSectionSpacing) {
            LogToolbarScrollRow {
                primaryControls
            }
            .controlSize(toolbarControlSize)

            LogToolbarScrollRow {
                toolbarActions
            }
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
            LogToolbarPickerField(title: t(.window), role: .window) {
                LogToolbarPopupPicker(
                    "Log window",
                    selection: $selectedLogPreset,
                    options: PodLogPreset.allCases.map { preset in
                        .init(value: preset, title: preset.title)
                    }
                )
                .frame(width: ResourceLogsLayoutMetrics.sourcePickerWidth)
                .runeMinimumInteractiveTarget()
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
                        selection: selectedPodID
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
                    .frame(width: ResourceLogsLayoutMetrics.sourcePickerWidth)
                    .runeMinimumInteractiveTarget()
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
        .buttonStyle(.plain)
        .accessibilityElement(children: .ignore)
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
        LogToolbarGroup(spacing: 6) {
            tailControl

            Button(action: onReload) {
                Label(t(.reload), systemImage: "arrow.clockwise")
            }
            .buttonStyle(.bordered)
            .logToolbarButtonFrame()
            .help(t(.reloadLogsHelp))

            Toggle(isOn: $includePreviousLogs) {
                toolbarIconLabel(t(.previous), systemImage: "clock.arrow.circlepath", help: t(.previousLogsHelp))
            }
            .toggleStyle(.button)
            .logToolbarIconButtonFrame()
            .help(t(.previousLogsHelp))
            .accessibilityLabel(t(.previous))

            Button(action: onSave) {
                Label(t(.saveLogs), systemImage: "square.and.arrow.down")
            }
            .buttonStyle(.borderedProminent)
            .logToolbarButtonFrame(width: 114)
            .help(t(.saveCurrentLogsHelp))

            Button(action: onSaveToExportFolder) {
                toolbarIconLabel(t(.quickSaveLogs), systemImage: "folder.badge.plus", help: t(.quickSaveLogsHelp))
            }
            .buttonStyle(.bordered)
            .logToolbarIconButtonFrame()
            .help(t(.quickSaveLogsHelp))
            .accessibilityLabel(t(.quickSaveLogs))
            .accessibilityIdentifier("log-quick-save")

            Menu {
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
            }
            .buttonStyle(.bordered)
            .logToolbarButtonFrame(width: 96)
            .help(t(.copyAndExportLogOutputHelp))
        }
        .fixedSize(horizontal: true, vertical: false)
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
    func logToolbarButtonFrame(width: CGFloat? = nil) -> some View {
        runeInterfaceFont(relativeSize: -1, weight: .semibold)
            .labelStyle(.titleAndIcon)
            .lineLimit(1)
            .frame(
                minWidth: width,
                idealWidth: width,
                maxWidth: width,
                minHeight: RuneUILayoutMetrics.inspectorToolbarControlMinHeight,
                idealHeight: RuneUILayoutMetrics.inspectorToolbarControlMinHeight,
                alignment: .center
            )
            .fixedSize(horizontal: true, vertical: false)
    }

    func logToolbarIconButtonFrame(
        width: CGFloat = RuneUILayoutMetrics.borderedIconButtonWidth
    ) -> some View {
        runeInterfaceFont(relativeSize: -1, weight: .semibold)
            .labelStyle(.iconOnly)
            .frame(
                minWidth: width,
                idealWidth: width,
                maxWidth: width,
                minHeight: RuneUILayoutMetrics.inspectorToolbarControlMinHeight,
                idealHeight: RuneUILayoutMetrics.inspectorToolbarControlMinHeight,
                alignment: .center
            )
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

    var body: some View {
        HStack(alignment: .center, spacing: spacing) {
            content
        }
        .padding(.horizontal, RuneUILayoutMetrics.inspectorToolbarGroupHorizontalPadding)
        .padding(.vertical, role.verticalPadding)
        .frame(minHeight: role.height)
        .background {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .fill(runeThemePalette?.row.opacity(0.42) ?? Color.primary.opacity(0.055))
        }
        .overlay {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .strokeBorder(
                    runeThemePalette?.stroke.opacity(0.28) ?? Color(nsColor: .separatorColor).opacity(0.16),
                    lineWidth: 1
                )
        }
        .fixedSize(horizontal: true, vertical: false)
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

    var body: some View {
        VStack(alignment: .leading, spacing: 5) {
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

    private var popupFont: NSFont {
        NSFont.systemFont(
            ofSize: RuneInterfaceTypography.appKitMenuFontSize(
                systemSmallFontSize: NSFont.smallSystemFontSize,
                interfaceMenuFontSize: interfaceMenuFontSize
            )
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
            popup.controlSize = .small
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
    @State private var isHovered = false
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        ZStack {
            Circle()
                .fill(statusColor.opacity(isHovered ? 0.28 : 0.18))
                .frame(width: RuneUILayoutMetrics.iconButtonSize, height: RuneUILayoutMetrics.iconButtonSize)

            Image(systemName: isTailModeEnabled && !isStreamPaused ? "pause.fill" : "play.fill")
                .runeInterfaceFont(relativeSize: -3, weight: .bold)
                .foregroundStyle(statusColor)
        }
        .frame(width: RuneUILayoutMetrics.borderedIconButtonWidth, height: RuneUILayoutMetrics.inspectorToolbarControlMinHeight)
        .contentShape(Rectangle())
        .onHover { isHovered = $0 }
        .fixedSize(horizontal: true, vertical: true)
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

    var body: some View {
        VStack(alignment: .leading, spacing: 5) {
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
                    .padding(.top, 8)
            } label: {
                summaryHeader
            }
        } else {
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(alignment: .center, spacing: 8) {
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
                .font(.caption.weight(.semibold))
                .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
        } content: {
            HStack(alignment: .center, spacing: 8) {
                Text(summary.isStructured ? "Structured logs" : "Repeated lines")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(runeThemePalette?.foreground ?? Color.primary)

                Text(summarySubtitle)
                    .font(.caption)
                    .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
                    .lineLimit(1)
                    .truncationMode(.middle)
            }
        }
        .fixedSize(horizontal: true, vertical: false)
    }

    private var summaryDetails: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(alignment: .center, spacing: 6) {
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
                RuneChip(horizontalPadding: 7, verticalPadding: 2) {
                    Label("\(duplicate.count)x", systemImage: "square.stack.3d.up")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(runeThemePalette?.accent ?? Color.accentColor)
                }
            }
            .buttonStyle(.plain)
            .runeMinimumInteractiveTarget()
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
        Menu {
            ForEach(field.sampleValues, id: \.self) { value in
                Button {
                    onSearchFieldSample(field, value)
                } label: {
                    Text(value)
                }
            }
        } label: {
            Label("\(field.title) \(field.nonEmptyCount)", systemImage: "number")
                .runeMinimumInteractiveTarget()
        }
        .menuStyle(.borderlessButton)
        .controlSize(.small)
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
        .padding(.horizontal, RuneUILayoutMetrics.inspectorControlContentInset)
        .padding(.vertical, RuneUILayoutMetrics.inspectorControlSurfaceVerticalPadding)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(RuneSurfaceBackground(kind: .inset))
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Log search and insights")
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
        let paneSpacing: CGFloat = presentationStyle == .terminalCompact
            ? RuneUILayoutMetrics.inspectorCompactSectionSpacing
            : RuneUILayoutMetrics.inspectorSectionSpacing

        VStack(alignment: .leading, spacing: paneSpacing) {
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
                interfaceLanguageRaw: interfaceLanguageRaw
            )
            .id(interfaceLanguageRaw)
            .frame(maxWidth: .infinity, alignment: .leading)

            ResourceLogsExplorePanel(
                searchQuery: $searchQuery,
                searchMatchCase: $searchMatchCase,
                selectedSearchMatchIndex: $selectedSearchMatchIndex,
                searchFocusRequestID: searchFocusRequestID,
                searchResult: activeSearchResult,
                structuredSummary: structuredLogSummary,
                showsInsights: !simpleMode,
                presentationStyle: presentationStyle,
                placeholder: t(.searchLogs),
                findHelp: t(.findInLogs),
                matchCaseHelp: t(.matchCase),
                onSearchFieldSample: { field, value in
                    applyLogSearchQuery(ResourceStructuredLogFieldSearch.query(field: field, value: value))
                },
                onSearchDuplicate: { duplicate in
                    applyLogSearchQuery(duplicate.fingerprint)
                },
                onSearchNavigate: {
                    searchNavigationSequence &+= 1
                }
            )
            .frame(maxWidth: .infinity, alignment: .leading)

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
        }
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
            HStack(spacing: 2) {
                TextField(
                    "",
                    text: $query,
                    prompt: Text(placeholder)
                        .foregroundStyle(runeThemePalette?.mutedText ?? Color.secondary)
                )
                .textFieldStyle(.plain)
                .font(.system(size: 12))
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
                    regularSearchAccessories
                    compactSearchAccessories
                }
                .layoutPriority(2)
            }
        }
        .frame(
            minWidth: ResourceLogsLayoutMetrics.searchChromeMinimumWidth,
            idealWidth: ResourceLogsLayoutMetrics.searchChromeIdealWidth,
            maxWidth: ResourceLogsLayoutMetrics.searchChromeMaximumWidth,
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

    private var regularSearchAccessories: some View {
        HStack(spacing: 2) {
            clearButton
            RuneMatchCaseButton(isSelected: $matchCase, help: matchCaseHelp)

            Rectangle()
                .fill(runeThemePalette?.divider ?? Color(nsColor: .separatorColor).opacity(0.35))
                .frame(width: 1, height: 16)
                .padding(.horizontal, 2)

            matchControls(statusWidth: ResourceLogsLayoutMetrics.searchMatchStatusWidth)
        }
        .fixedSize(horizontal: true, vertical: false)
    }

    private var compactSearchAccessories: some View {
        HStack(spacing: 0) {
            matchStatusButton(width: ResourceLogsLayoutMetrics.compactSearchMatchStatusWidth)

            Menu {
                Button("Clear search", systemImage: "xmark.circle") {
                    query = ""
                }
                .disabled(normalizedQuery.isEmpty)

                Button(matchCase ? "Disable Match Case" : "Enable Match Case", systemImage: "textformat") {
                    matchCase.toggle()
                }

                Divider()

                Button("Previous match", systemImage: "chevron.up") {
                    selectPreviousMatch()
                }
                .disabled(searchSummary?.hasMatches != true)

                Button("Next match", systemImage: "chevron.down") {
                    selectNextMatch()
                }
                .disabled(searchSummary?.hasMatches != true)
            } label: {
                Image(systemName: "ellipsis.circle")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
                    .frame(
                        width: RuneUILayoutMetrics.iconButtonSize,
                        height: RuneUILayoutMetrics.iconButtonSize
                    )
                    .contentShape(Rectangle())
            }
            .menuStyle(.borderlessButton)
            .menuIndicator(.hidden)
            .fixedSize(horizontal: true, vertical: false)
            .help("Log search options")
            .accessibilityLabel("Log search options")
        }
        .fixedSize(horizontal: true, vertical: false)
        .accessibilityIdentifier("resource-log-search-compact-controls")
    }

    private var clearButton: some View {
        RuneIconButton(
            "Clear log search",
            systemImage: "xmark.circle.fill",
            isDisabled: normalizedQuery.isEmpty
        ) {
            query = ""
        }
        .opacity(normalizedQuery.isEmpty ? 0 : 1)
        .allowsHitTesting(!normalizedQuery.isEmpty)
        .accessibilityHidden(normalizedQuery.isEmpty)
    }

    private func matchControls(statusWidth: CGFloat) -> some View {
        HStack(spacing: 0) {
            matchStatusButton(width: statusWidth)

            RuneIconButton(
                "Previous match",
                systemImage: "chevron.up",
                isDisabled: searchSummary?.matchRanges.isEmpty != false
            ) {
                selectPreviousMatch()
            }

            RuneIconButton(
                "Next match",
                systemImage: "chevron.down",
                isDisabled: searchSummary?.matchRanges.isEmpty != false
            ) {
                selectNextMatch()
            }
        }
        .fixedSize(horizontal: true, vertical: false)
        .accessibilityElement(children: .contain)
        .accessibilityIdentifier("resource-log-search-match-controls")
    }

    private func matchStatusButton(width: CGFloat) -> some View {
        Button {
            guard let searchSummary else { return }
            prepareJumpPopover(for: searchSummary)
        } label: {
            Text(matchStatusText)
                .font(.caption.weight(.medium))
                .foregroundStyle(matchStatusColor)
                .monospacedDigit()
                .lineLimit(1)
                .minimumScaleFactor(0.72)
                .frame(
                    width: width,
                    height: RuneUILayoutMetrics.iconButtonSize,
                    alignment: .trailing
                )
        }
        .buttonStyle(.plain)
        .disabled(searchSummary?.hasMatches != true)
        .popover(isPresented: $isJumpPopoverPresented, arrowEdge: .bottom) {
            if jumpMatchCount > 0 {
                jumpToMatchPopover(matchCount: jumpMatchCount)
            }
        }
        .help("Go to match number")
        .accessibilityLabel(matchStatusAccessibilityLabel)
        .accessibilityIdentifier("resource-log-search-match-status")
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
        VStack(alignment: .leading, spacing: 10) {
            Text("Go to match")
                .font(.headline)

            HStack(spacing: 8) {
                TextField("Match", text: $jumpText)
                    .textFieldStyle(.roundedBorder)
                    .font(.system(size: 12, weight: .regular, design: .monospaced))
                    .frame(width: 82)
                    .onSubmit {
                        commitJump(matchCount: matchCount)
                    }
                    .runeTextInputCursor()

                Text("of \(matchCount)")
                    .font(.caption.weight(.medium))
                    .foregroundStyle(.secondary)
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
        .padding(12)
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
