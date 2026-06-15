import Foundation
import struct RuneSharedCore.RuneLargeTextIndex
import RuneCore
import SwiftUI

enum ResourceLogsPresentationStyle: Hashable {
    case regular
    case terminalCompact
}

struct ResourceLogsToolbar: View {
    @Binding var selectedLogPreset: PodLogPreset
    @Binding var includePreviousLogs: Bool
    @Binding var selectedContainer: String
    @Binding var isTailModeEnabled: Bool
    @Binding var isStreamPaused: Bool
    @Binding var searchQuery: String
    @Binding var searchMatchCase: Bool
    @Binding var selectedSearchMatchIndex: Int
    let searchPulseID: Int
    let searchSummary: ResourceLogSearchResult?
    let statusText: String
    var podOptions: [PodSummary] = []
    var selectedPodID: Binding<String>? = nil
    var isFavoritePod: (PodSummary) -> Bool = { _ in false }
    var onToggleFavoritePod: (PodSummary) -> Void = { _ in }
    var presentationStyle: ResourceLogsPresentationStyle = .regular
    var showsContainerPicker: Bool = true
    let containerOptions: [String]
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

    var body: some View {
        switch presentationStyle {
        case .regular:
            regularBody
        case .terminalCompact:
            terminalCompactBody
        }
    }

    private var regularBody: some View {
        VStack(alignment: .leading, spacing: 9) {
            LogToolbarScrollRow {
                primaryControls
            }
            .controlSize(.small)

            LogToolbarScrollRow {
                toolbarActions
            }
            .controlSize(.small)

            ResourceLogsSearchBar(
                query: $searchQuery,
                matchCase: $searchMatchCase,
                selectedMatchIndex: $selectedSearchMatchIndex,
                searchPulseID: searchPulseID,
                searchSummary: searchSummary,
                placeholder: t(.searchLogs),
                findHelp: t(.findInLogs),
                matchCaseHelp: t(.matchCase)
            )
        }
    }

    private var terminalCompactBody: some View {
        VStack(alignment: .leading, spacing: 7) {
            LogToolbarScrollRow {
                primaryControls
            }
            .controlSize(.small)

            LogToolbarScrollRow {
                toolbarActions
            }
            .controlSize(.small)

            ResourceLogsSearchBar(
                query: $searchQuery,
                matchCase: $searchMatchCase,
                selectedMatchIndex: $selectedSearchMatchIndex,
                searchPulseID: searchPulseID,
                searchSummary: searchSummary,
                placeholder: t(.searchLogs),
                findHelp: t(.findInLogs),
                matchCaseHelp: t(.matchCase)
            )
            .frame(minWidth: 240, maxWidth: .infinity)
            .layoutPriority(1)
        }
    }

    private var sourceControls: some View {
        LogToolbarGroup(role: .source) {
            LogToolbarPickerField(title: t(.window), role: .window) {
                Picker("Log window", selection: $selectedLogPreset) {
                    ForEach(PodLogPreset.allCases) { preset in
                        Text(preset.title).tag(preset)
                    }
                }
                .labelsHidden()
                .frame(width: 158)
            }

            if let selectedPodID, !podOptions.isEmpty {
                LogToolbarPickerField(title: t(.pod), role: .pod) {
                    FavoritePodPicker(
                        title: t(.pod),
                        pods: podOptions,
                        width: 240,
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
                    Picker("Container", selection: $selectedContainer) {
                        Text(t(.allContainers)).tag("")
                        ForEach(containerOptions, id: \.self) { container in
                            Text(container).tag(container)
                        }
                    }
                    .labelsHidden()
                    .frame(width: 166)
                    .help(t(.chooseContainerHelp))
                }
            }
        }
    }

    private var primaryControls: some View {
        HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarGroupSpacing) {
            sourceControls
        }
        .fixedSize(horizontal: true, vertical: false)
    }

    @ViewBuilder
    private var tailControl: some View {
        if isTailModeEnabled {
            if isStreamPaused {
                Button(action: toggleTailPlayback) {
                    toolbarIconLabel(t(.resume), systemImage: "play.fill", help: t(.resumeTailHelp))
                }
                .buttonStyle(.bordered)
                .logToolbarIconButtonFrame()
                .contextMenu {
                    Button(t(.stopTail)) {
                        isTailModeEnabled = false
                        isStreamPaused = false
                    }
                }
                .help(t(.resumeTailHelp))
            } else {
                Button(action: toggleTailPlayback) {
                    toolbarIconLabel(t(.pause), systemImage: "pause.fill", help: t(.pauseTailHelp))
                }
                .buttonStyle(.borderedProminent)
                .logToolbarIconButtonFrame()
                .contextMenu {
                    Button(t(.stopTail)) {
                        isTailModeEnabled = false
                        isStreamPaused = false
                    }
                }
                .help(t(.pauseTailHelp))
            }
        } else {
            Button(action: toggleTailPlayback) {
                toolbarIconLabel(t(.tail), systemImage: "play.fill", help: t(.startTailHelp))
            }
            .buttonStyle(.bordered)
            .logToolbarIconButtonFrame()
            .help(t(.startTailHelp))
        }
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
            LogToolbarStatusIndicator(
                statusText: statusText,
                showsPreviousHint: includePreviousLogs
            )

            Button(action: onReload) {
                Label(t(.reload), systemImage: "arrow.clockwise")
            }
            .buttonStyle(.bordered)
            .logToolbarButtonFrame()
            .help(t(.reloadLogsHelp))

            Button(action: onSave) {
                Label(t(.saveLogs), systemImage: "square.and.arrow.down")
            }
            .buttonStyle(.borderedProminent)
            .logToolbarButtonFrame(width: 114)
            .help(t(.saveCurrentLogsHelp))

            Toggle(isOn: $includePreviousLogs) {
                toolbarIconLabel(t(.previous), systemImage: "clock.arrow.circlepath", help: t(.previousLogsHelp))
            }
                .toggleStyle(.button)
                .logToolbarIconButtonFrame()
                .help(t(.previousLogsHelp))
                .accessibilityLabel(t(.previous))

            tailControl

            Menu {
                Button(action: onCopySelection) {
                    Label(t(.copySelection), systemImage: "doc.on.doc")
                }
                Button(action: onCopyAll) {
                    Label(t(.copyAll), systemImage: "doc.on.clipboard")
                }

                Divider()

                Button(action: onSaveToExportFolder) {
                    Label("Save to Export Folder", systemImage: "folder")
                }
                Button(action: onSaveAndOpen) {
                    Label("Save and Open", systemImage: "arrow.up.right.square")
                }

                Divider()

                Button {
                    onSaveVisibleZip(searchSummary?.displayedText ?? "")
                } label: {
                    Label(t(.exportVisibleResultsZip), systemImage: "doc.zipper")
                }
                Button {
                    onSaveVisibleZipToExportFolder(searchSummary?.displayedText ?? "")
                } label: {
                    Label("Save Visible ZIP to Export Folder", systemImage: "folder.badge.plus")
                }
                Button {
                    onSaveVisibleZipAndOpen(searchSummary?.displayedText ?? "")
                } label: {
                    Label("Save Visible ZIP and Open", systemImage: "archivebox")
                }
                Button(action: onSaveFullZip) {
                    Label(t(.exportFullUnfilteredZip), systemImage: "archivebox")
                }
                Button(action: onSaveFullZipToExportFolder) {
                    Label("Save Full ZIP to Export Folder", systemImage: "folder.badge.plus")
                }
                Button(action: onSaveFullZipAndOpen) {
                    Label("Save Full ZIP and Open", systemImage: "archivebox")
                }
                Button(action: onSaveAllPodsZip) {
                    Label(t(.exportAllPodsFullZip), systemImage: "shippingbox")
                }
                Button(action: onSaveAllPodsZipToExportFolder) {
                    Label("Save All Pods ZIP to Export Folder", systemImage: "folder.badge.plus")
                }
                Button(action: onSaveAllPodsZipAndOpen) {
                    Label("Save All Pods ZIP and Open", systemImage: "archivebox")
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
        font(.caption.weight(.semibold))
            .labelStyle(.titleAndIcon)
            .lineLimit(1)
            .frame(
                minWidth: width,
                idealWidth: width,
                maxWidth: width,
                minHeight: RuneUILayoutMetrics.inspectorToolbarControlMinHeight,
                idealHeight: RuneUILayoutMetrics.inspectorToolbarControlMinHeight,
                maxHeight: RuneUILayoutMetrics.inspectorToolbarControlMinHeight,
                alignment: .center
            )
            .fixedSize(horizontal: true, vertical: false)
    }

    func logToolbarIconButtonFrame(width: CGFloat = 32) -> some View {
        font(.caption.weight(.semibold))
            .labelStyle(.iconOnly)
            .frame(
                minWidth: width,
                idealWidth: width,
                maxWidth: width,
                minHeight: RuneUILayoutMetrics.inspectorToolbarControlMinHeight,
                idealHeight: RuneUILayoutMetrics.inspectorToolbarControlMinHeight,
                maxHeight: RuneUILayoutMetrics.inspectorToolbarControlMinHeight,
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
            return 6
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
        .frame(height: role.height)
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

    var body: some View {
        VStack(alignment: .leading, spacing: 5) {
            Text(title.uppercased())
                .font(.caption2.weight(.bold))
                .foregroundStyle(.secondary)
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
            return 240
        case .container:
            return 166
        case .window:
            return 158
        }
    }

}

private struct LogToolbarStatusIndicator: View {
    let statusText: String
    let showsPreviousHint: Bool

    var body: some View {
        ZStack {
            Circle()
                .fill(statusColor.opacity(0.18))
                .frame(width: 18, height: 18)

            Circle()
                .fill(statusColor)
                .frame(width: 7, height: 7)
        }
        .frame(width: 20, height: RuneUILayoutMetrics.inspectorToolbarControlMinHeight)
        .help(helpText)
        .accessibilityLabel("Log status: \(statusText)")
        .fixedSize(horizontal: true, vertical: true)
    }

    private var helpText: String {
        if showsPreviousHint {
            return "\(statusText). Previous logs only exist for restarted containers."
        }
        return statusText
    }

    private var statusColor: Color {
        let normalized = statusText.lowercased()
        if normalized.contains("failed") || normalized.contains("error") {
            return .red
        }
        if normalized.contains("loading") || normalized.contains("reloading") {
            return .blue
        }
        if normalized.contains("paused") {
            return .yellow
        }
        return .green
    }
}

private struct ResourceLogsSourcePanel: View {
    let title: String
    let values: [String]

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: 8) {
            Image(systemName: "square.stack.3d.forward.dottedline")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
                .frame(width: 14)

            Text(title.uppercased())
                .font(.caption2.weight(.semibold))
                .foregroundStyle(.secondary)
                .tracking(0.3)

            Text(values.joined(separator: ", "))
                .font(.caption.weight(.medium))
                .foregroundStyle(.primary)
                .lineLimit(1)
                .truncationMode(.middle)

            Spacer(minLength: 0)
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 7)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background {
            RoundedRectangle(cornerRadius: 8, style: .continuous)
                .fill(Color.primary.opacity(0.035))
        }
        .overlay {
            RoundedRectangle(cornerRadius: 8, style: .continuous)
                .strokeBorder(Color(nsColor: .separatorColor).opacity(0.16), lineWidth: 1)
        }
        .accessibilityLabel("\(title): \(values.joined(separator: ", "))")
    }
}

private struct ResourceStructuredLogSummaryPanel: View {
    let summary: ResourceStructuredLogSummary
    let onSearchFieldSample: (ResourceStructuredLogField, String) -> Void
    let onSearchDuplicate: (ResourceDuplicateLogLine) -> Void

    var body: some View {
        if summary.isStructured || !summary.duplicateLines.isEmpty {
            VStack(alignment: .leading, spacing: 8) {
                HStack(alignment: .center, spacing: 8) {
                    Image(systemName: summary.isStructured ? "curlybraces" : "text.line.first.and.arrowtriangle.forward")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)
                        .frame(width: 14)

                    Text(summary.isStructured ? "Structured logs" : "Repeated lines")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.primary)

                    Text(summarySubtitle)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                        .truncationMode(.middle)

                    Spacer(minLength: 0)
                }

                if !summary.fields.isEmpty {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 6) {
                            ForEach(summary.fields.prefix(6), id: \.key) { field in
                                fieldMenu(field)
                            }
                        }
                        .fixedSize(horizontal: true, vertical: false)
                    }
                }

                if !summary.duplicateLines.isEmpty {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 6) {
                            ForEach(summary.duplicateLines.prefix(4), id: \.fingerprint) { duplicate in
                                Button {
                                    onSearchDuplicate(duplicate)
                                } label: {
                                    Label("\(duplicate.count)x", systemImage: "square.stack.3d.up")
                                }
                                .buttonStyle(.bordered)
                                .controlSize(.small)
                                .help("Search repeated log line: \(duplicate.fingerprint)")
                            }
                        }
                        .fixedSize(horizontal: true, vertical: false)
                    }
                }
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 8)
            .background {
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .fill(Color.primary.opacity(0.035))
            }
            .overlay {
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .strokeBorder(Color(nsColor: .separatorColor).opacity(0.16), lineWidth: 1)
            }
            .accessibilityLabel(summarySubtitle)
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
        }
        .menuStyle(.borderlessButton)
        .controlSize(.small)
        .help("Search structured field \(field.title)")
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
    @AppStorage(RuneSettingsKeys.simpleMode) private var simpleMode = false
    @AppStorage(RuneSettingsKeys.interfaceLanguage) private var interfaceLanguageRaw =
        RuneSettingsKeys.interfaceLanguageDefault
    @State private var searchQuery = ""
    @State private var searchMatchCase = false
    @State private var selectedSearchMatchIndex = 0
    @State private var searchNavigationSequence = 0
    @State private var searchPulseID = 0
    @State private var structuredLogSummary = ResourceStructuredLogAnalyzer.analyze(text: "")

    var body: some View {
        let searchResult = ResourceLogSearchResult.makeForInspector(
            text: logText,
            query: searchQuery,
            matchCase: searchMatchCase
        )
        let resolvedSearchMatchIndex = searchResult.clampedMatchIndex(selectedSearchMatchIndex)
        let paneSpacing: CGFloat = presentationStyle == .terminalCompact ? 8 : 10

        VStack(alignment: .leading, spacing: paneSpacing) {
            ResourceLogsToolbar(
                selectedLogPreset: $selectedLogPreset,
                includePreviousLogs: $includePreviousLogs,
                selectedContainer: $selectedContainer,
                isTailModeEnabled: $isTailModeEnabled,
                isStreamPaused: $isStreamPaused,
                searchQuery: $searchQuery,
                searchMatchCase: $searchMatchCase,
                selectedSearchMatchIndex: $selectedSearchMatchIndex,
                searchPulseID: searchPulseID,
                searchSummary: searchResult,
                statusText: statusText,
                podOptions: podOptions,
                selectedPodID: selectedPodID,
                isFavoritePod: isFavoritePod,
                onToggleFavoritePod: onToggleFavoritePod,
                presentationStyle: presentationStyle,
                showsContainerPicker: showsContainerPicker,
                containerOptions: containerOptions,
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
                onToggleStreamPause: onToggleStreamPause,
                interfaceLanguageRaw: interfaceLanguageRaw
            )
            .id(interfaceLanguageRaw)

            if !simpleMode {
                ResourceStructuredLogSummaryPanel(
                    summary: structuredLogSummary,
                    onSearchFieldSample: { field, value in
                        applyLogSearchQuery(ResourceStructuredLogFieldSearch.query(field: field, value: value))
                    },
                    onSearchDuplicate: { duplicate in
                        applyLogSearchQuery(duplicate.fingerprint)
                    }
                )
            }

            ResourceLogsOutputSurface(
                isLoadingLogs: isLoadingLogs,
                isLoadingResources: isLoadingResources,
                errorMessage: errorMessage,
                searchResult: searchResult,
                selectedSearchMatchIndex: resolvedSearchMatchIndex,
                searchNavigationSequence: searchNavigationSequence,
                emptyTitle: "No log output",
                emptyMessage: "The pod may be idle, or the current filter returned no lines.",
                noMatchesMessage: "No log lines matched the current search.",
                readOnlyResetID: readOnlyResetID,
                onReload: onReload
            )
        }
        .onChange(of: searchQuery) { _, _ in
            selectedSearchMatchIndex = 0
            searchNavigationSequence += 1
        }
        .onChange(of: searchMatchCase) { _, _ in
            selectedSearchMatchIndex = 0
            searchNavigationSequence += 1
        }
        .onChange(of: logText) { _, _ in
            selectedSearchMatchIndex = searchResult.clampedMatchIndex(selectedSearchMatchIndex)
        }
        .task(id: "\(simpleMode):\(logText)") {
            guard !simpleMode else {
                structuredLogSummary = ResourceStructuredLogAnalyzer.analyze(text: "")
                return
            }
            await refreshStructuredSummary(for: logText)
        }
    }

    private func refreshStructuredSummary(for text: String) async {
        let summary = await Task.detached(priority: .utility) {
            ResourceStructuredLogAnalyzer.analyze(text: text)
        }.value
        guard !Task.isCancelled else { return }
        structuredLogSummary = summary
    }

    private func applyLogSearchQuery(_ query: String) {
        searchQuery = query
        searchPulseID += 1
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
    @AppStorage(RuneSettingsKeys.simpleMode) private var simpleMode = false
    @AppStorage(RuneSettingsKeys.interfaceLanguage) private var interfaceLanguageRaw =
        RuneSettingsKeys.interfaceLanguageDefault
    @State private var searchQuery = ""
    @State private var searchMatchCase = false
    @State private var selectedSearchMatchIndex = 0
    @State private var searchNavigationSequence = 0
    @State private var searchPulseID = 0
    @State private var structuredLogSummary = ResourceStructuredLogAnalyzer.analyze(text: "")

    var body: some View {
        let searchResult = ResourceLogSearchResult.makeForInspector(
            text: logText,
            query: searchQuery,
            matchCase: searchMatchCase
        )
        let resolvedSearchMatchIndex = searchResult.clampedMatchIndex(selectedSearchMatchIndex)

        VStack(alignment: .leading, spacing: 10) {
            ResourceLogsToolbar(
                selectedLogPreset: $selectedLogPreset,
                includePreviousLogs: $includePreviousLogs,
                selectedContainer: .constant(""),
                isTailModeEnabled: $isTailModeEnabled,
                isStreamPaused: $isStreamPaused,
                searchQuery: $searchQuery,
                searchMatchCase: $searchMatchCase,
                selectedSearchMatchIndex: $selectedSearchMatchIndex,
                searchPulseID: searchPulseID,
                searchSummary: searchResult,
                statusText: statusText,
                podOptions: [],
                selectedPodID: nil,
                showsContainerPicker: false,
                containerOptions: [],
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
                onToggleStreamPause: onToggleStreamPause,
                interfaceLanguageRaw: interfaceLanguageRaw
            )
            .id(interfaceLanguageRaw)

            if !podNames.isEmpty {
                ResourceLogsSourcePanel(title: "Pods", values: podNames)
            }

            if !simpleMode {
                ResourceStructuredLogSummaryPanel(
                    summary: structuredLogSummary,
                    onSearchFieldSample: { field, value in
                        applyLogSearchQuery(ResourceStructuredLogFieldSearch.query(field: field, value: value))
                    },
                    onSearchDuplicate: { duplicate in
                        applyLogSearchQuery(duplicate.fingerprint)
                    }
                )
            }

            ResourceLogsOutputSurface(
                isLoadingLogs: isLoadingLogs,
                isLoadingResources: isLoadingResources,
                errorMessage: errorMessage,
                searchResult: searchResult,
                selectedSearchMatchIndex: resolvedSearchMatchIndex,
                searchNavigationSequence: searchNavigationSequence,
                emptyTitle: "No log output",
                emptyMessage: "No lines were returned for the selected pods and the current filter. Pods may be idle or produce no output for this time window.",
                noMatchesMessage: "No unified log lines matched the current search.",
                readOnlyResetID: readOnlyResetID,
                onReload: onReload
            )
        }
        .onChange(of: searchQuery) { _, _ in
            selectedSearchMatchIndex = 0
            searchNavigationSequence += 1
        }
        .onChange(of: searchMatchCase) { _, _ in
            selectedSearchMatchIndex = 0
            searchNavigationSequence += 1
        }
        .onChange(of: logText) { _, _ in
            selectedSearchMatchIndex = searchResult.clampedMatchIndex(selectedSearchMatchIndex)
        }
        .task(id: "\(simpleMode):\(logText)") {
            guard !simpleMode else {
                structuredLogSummary = ResourceStructuredLogAnalyzer.analyze(text: "")
                return
            }
            await refreshStructuredSummary(for: logText)
        }
    }

    private func refreshStructuredSummary(for text: String) async {
        let summary = await Task.detached(priority: .utility) {
            ResourceStructuredLogAnalyzer.analyze(text: text)
        }.value
        guard !Task.isCancelled else { return }
        structuredLogSummary = summary
    }

    private func applyLogSearchQuery(_ query: String) {
        searchQuery = query
        searchPulseID += 1
    }
}

private struct ResourceLogsOutputSurface: View {
    let isLoadingLogs: Bool
    let isLoadingResources: Bool
    let errorMessage: String?
    let searchResult: ResourceLogSearchResult
    let selectedSearchMatchIndex: Int
    let searchNavigationSequence: Int
    let emptyTitle: String
    let emptyMessage: String
    let noMatchesMessage: String
    let readOnlyResetID: String
    let onReload: () -> Void

    var body: some View {
        InspectorTextSurface(minHeight: 280) {
            Group {
                if isLoadingLogs || isLoadingResources {
                    ResourceLogsLoadingPlaceholder()
                } else if let errorMessage {
                    ResourceLogsErrorView(message: errorMessage, onReload: onReload)
                } else if searchResult.originalText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    ResourceLogsEmptyPlaceholder(title: emptyTitle, message: emptyMessage)
                } else {
                    VStack(alignment: .leading, spacing: 0) {
                        if searchResult.isFiltering {
                            ResourceLogsSearchSummaryBar(
                                searchResult: searchResult,
                                selectedMatchIndex: selectedSearchMatchIndex
                            )
                        }

                        let outputResetID = "\(readOnlyResetID):\(searchResult.scrollIdentityToken)"
                        let navigationRequest = searchResult.navigationRequest(
                            selectedIndex: selectedSearchMatchIndex,
                            sequence: searchNavigationSequence
                        )
                        let usesLargeTextSurface = ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: searchResult)
                        InspectorReadOnlyTextView(
                            text: searchResult.displayedText,
                            resetID: outputResetID,
                            resetScrollOnExternalChange: false,
                            contentStyle: .ansiLogs,
                            navigationRequest: usesLargeTextSurface ? nil : navigationRequest,
                            usesLargeTextSurface: usesLargeTextSurface,
                            allowsAutomaticLargeTextSurface: false,
                            largeTextIndex: searchResult.textIndex,
                            largeTextScrollTargetLine: searchResult.matchLineNumber(selectedIndex: selectedSearchMatchIndex),
                            largeTextScrollTargetRevision: searchResult.largeTextNavigationRevision(
                                selectedIndex: selectedSearchMatchIndex,
                                sequence: searchNavigationSequence
                            ),
                            largeTextShowsLineNumbers: false
                        )
                    }
                }
            }
        }
    }
}

enum ResourceLogsDeferredRenderingPolicy {
    static let deferredOutputThreshold = 250_000
    static let deferredLineCountThreshold = 1_000

    static func shouldDeferOutputMount(for result: ResourceLogSearchResult) -> Bool {
        result.displayedText.utf8.count > deferredOutputThreshold
            && result.textIndex.lineCount >= deferredLineCountThreshold
    }
}

private enum LogSearchBarMetrics {
    static let widestBadgePlaceholder = "9,999,999 matches"
    static let widestPositionPlaceholder = "9,999,999 of 9,999,999"
}

private struct ResourceLogsSearchBar: View {
    @Binding var query: String
    @Binding var matchCase: Bool
    @Binding var selectedMatchIndex: Int
    let searchPulseID: Int
    let searchSummary: ResourceLogSearchResult?
    let placeholder: String
    let findHelp: String
    let matchCaseHelp: String
    @State private var draftQuery = ""
    @State private var queryCommitTask: Task<Void, Never>?
    @State private var pulseOpacity = 0.0
    @State private var isJumpPopoverPresented = false
    @State private var jumpText = ""
    @FocusState private var isSearchFocused: Bool

    var body: some View {
        HStack(spacing: 10) {
            HStack(spacing: 8) {
                Button {
                    isSearchFocused = true
                } label: {
                    Image(systemName: "magnifyingglass")
                        .foregroundStyle(.secondary)
                }
                .buttonStyle(.plain)
                .keyboardShortcut("f", modifiers: [.command])
                .help(findHelp)

                TextField(placeholder, text: $draftQuery)
                    .textFieldStyle(.plain)
                    .font(.system(size: 12, weight: .regular, design: .monospaced))
                    .focused($isSearchFocused)
                    .onSubmit {
                        if let searchSummary {
                            selectedMatchIndex = searchSummary.nextMatchIndex(from: selectedMatchIndex)
                        }
                    }
                if !draftQuery.isEmpty {
                    Button {
                        queryCommitTask?.cancel()
                        draftQuery = ""
                        query = ""
                    } label: {
                        Image(systemName: "xmark.circle.fill")
                            .foregroundStyle(.secondary)
                    }
                    .buttonStyle(.plain)
                }

                Button {
                    matchCase.toggle()
                } label: {
                    Text("Aa")
                        .font(.caption.weight(.semibold))
                        .frame(width: 24, height: 20)
                }
                .buttonStyle(.plain)
                .background(
                    RoundedRectangle(cornerRadius: 5, style: .continuous)
                        .fill(matchCase ? Color.accentColor.opacity(0.22) : Color.clear)
                )
                .help(matchCaseHelp)
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 8)
            .background {
                RoundedRectangle(cornerRadius: 10, style: .continuous)
                    .fill(Color.primary.opacity(0.05))
            }
            .overlay {
                RoundedRectangle(cornerRadius: 10, style: .continuous)
                    .strokeBorder(Color(nsColor: .separatorColor).opacity(0.18), lineWidth: 1)
            }
            .overlay {
                LogSearchPulseOverlay(opacity: pulseOpacity)
            }
            .onAppear {
                draftQuery = query
            }
            .onDisappear {
                queryCommitTask?.cancel()
            }
            .onChange(of: draftQuery) { _, newValue in
                queryCommitTask?.cancel()
                queryCommitTask = Task { @MainActor in
                    try? await Task.sleep(nanoseconds: 85_000_000)
                    guard !Task.isCancelled, query != newValue else { return }
                    query = newValue
                }
            }
            .onChange(of: query) { _, newValue in
                guard draftQuery != newValue else { return }
                draftQuery = newValue
            }
            .onChange(of: searchPulseID) { _, _ in
                flashSearchPulse()
            }

            if let searchSummary, searchSummary.isFiltering {
                HStack(spacing: 6) {
                    Button {
                        selectedMatchIndex = searchSummary.previousMatchIndex(from: selectedMatchIndex)
                    } label: {
                        Image(systemName: "chevron.up")
                            .font(.system(size: 12, weight: .medium))
                            .foregroundStyle(.secondary)
                            .frame(width: 22, height: 22)
                            .contentShape(Rectangle())
                    }
                    .buttonStyle(.plain)
                    .disabled(searchSummary.matchRanges.isEmpty)
                    .help("Previous match")

                    Button {
                        selectedMatchIndex = searchSummary.nextMatchIndex(from: selectedMatchIndex)
                    } label: {
                        Image(systemName: "chevron.down")
                            .font(.system(size: 12, weight: .medium))
                            .foregroundStyle(.secondary)
                            .frame(width: 22, height: 22)
                            .contentShape(Rectangle())
                    }
                    .buttonStyle(.plain)
                    .disabled(searchSummary.matchRanges.isEmpty)
                    .help("Next match")

                    ZStack {
                        Text(LogSearchBarMetrics.widestBadgePlaceholder)
                            .font(.caption.weight(.semibold))
                            .lineLimit(1)
                            .hidden()
                        Text(searchSummary.badgeText)
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(searchSummary.matchingLineCount == 0 ? .secondary : .primary)
                            .lineLimit(1)
                            .minimumScaleFactor(0.75)
                    }
                    .frame(height: 22)

                    Button {
                        prepareJumpPopover(for: searchSummary)
                    } label: {
                        ZStack {
                            Text(LogSearchBarMetrics.widestPositionPlaceholder)
                                .font(.caption.weight(.semibold))
                                .monospacedDigit()
                                .lineLimit(1)
                                .hidden()
                            Text(
                                searchSummary.hasMatches
                                    ? searchSummary.matchPositionText(selectedIndex: selectedMatchIndex)
                                    : "\u{00a0}"
                            )
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(searchSummary.hasMatches ? Color.secondary : Color.clear)
                            .monospacedDigit()
                            .lineLimit(1)
                            .minimumScaleFactor(0.75)
                        }
                        .frame(height: 22)
                    }
                    .buttonStyle(.plain)
                    .disabled(!searchSummary.hasMatches)
                    .popover(isPresented: $isJumpPopoverPresented, arrowEdge: .bottom) {
                        jumpToMatchPopover(searchSummary)
                    }
                    .help("Go to match number")
                    .accessibilityLabel(
                        searchSummary.hasMatches
                            ? "Search match \(searchSummary.matchPositionText(selectedIndex: selectedMatchIndex))"
                            : "Search match position"
                    )
                }
                .padding(.horizontal, 8)
                .padding(.vertical, 6)
                .background {
                    RoundedRectangle(cornerRadius: 9, style: .continuous)
                        .fill(Color.primary.opacity(0.05))
                }
                .overlay {
                    RoundedRectangle(cornerRadius: 9, style: .continuous)
                        .strokeBorder(Color(nsColor: .separatorColor).opacity(0.18), lineWidth: 1)
                }
                .fixedSize(horizontal: true, vertical: false)
            }
        }
    }

    private func prepareJumpPopover(for searchSummary: ResourceLogSearchResult) {
        guard searchSummary.hasMatches else { return }
        jumpText = "\(searchSummary.clampedMatchIndex(selectedMatchIndex) + 1)"
        isJumpPopoverPresented = true
    }

    private func jumpToMatchPopover(_ searchSummary: ResourceLogSearchResult) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Go to match")
                .font(.headline)

            HStack(spacing: 8) {
                TextField("Match", text: $jumpText)
                    .textFieldStyle(.roundedBorder)
                    .font(.system(size: 12, weight: .regular, design: .monospaced))
                    .frame(width: 82)
                    .onSubmit {
                        commitJump(to: searchSummary)
                    }

                Text("of \(searchSummary.matchRanges.count)")
                    .font(.caption.weight(.medium))
                    .foregroundStyle(.secondary)
                    .monospacedDigit()
            }

            HStack {
                Spacer()
                Button("Go") {
                    commitJump(to: searchSummary)
                }
                .keyboardShortcut(.defaultAction)
            }
        }
        .padding(12)
        .frame(width: 180)
    }

    private func commitJump(to searchSummary: ResourceLogSearchResult) {
        guard searchSummary.hasMatches else { return }
        let requestedMatch = Int(jumpText.trimmingCharacters(in: .whitespacesAndNewlines)) ?? 1
        selectedMatchIndex = min(max(requestedMatch, 1), searchSummary.matchRanges.count) - 1
        isJumpPopoverPresented = false
    }

    private func flashSearchPulse() {
        withAnimation(.easeOut(duration: 0.08)) {
            pulseOpacity = 1
        }
        Task { @MainActor in
            try? await Task.sleep(nanoseconds: 260_000_000)
            guard !Task.isCancelled else { return }
            withAnimation(.easeOut(duration: 0.24)) {
                pulseOpacity = 0
            }
        }
    }

}

private struct LogSearchPulseOverlay: View {
    let opacity: Double

    var body: some View {
        RoundedRectangle(cornerRadius: 10, style: .continuous)
            .strokeBorder(Color.accentColor.opacity(0.32 * opacity), lineWidth: 2)
            .shadow(color: Color.accentColor.opacity(0.18 * opacity), radius: 6)
            .allowsHitTesting(false)
    }
}

private struct ResourceLogsSearchSummaryBar: View {
    let searchResult: ResourceLogSearchResult
    let selectedMatchIndex: Int

    var body: some View {
        HStack {
            Text(searchResult.summaryText)
                .font(.footnote.weight(.medium))
                .foregroundStyle(.secondary)
            Spacer()
            if searchResult.hasMatches {
                Text(searchResult.matchPositionText(selectedIndex: selectedMatchIndex))
                    .font(.footnote.weight(.semibold))
                    .foregroundStyle(.secondary)
                    .monospacedDigit()
            }
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 8)
        .background(Color.primary.opacity(0.035))
        .overlay(alignment: .bottom) {
            Rectangle()
                .fill(Color(nsColor: .separatorColor).opacity(0.18))
                .frame(height: 1)
        }
    }
}

struct ResourceLogSearchResult: Equatable {
    let originalText: String
    let displayedText: String
    let query: String
    let matchCase: Bool
    let totalLineCount: Int
    let matchingLineCount: Int
    let matchRanges: [NSRange]
    let matchLineNumbers: [Int]
    let textIndex: RuneLargeTextIndex

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
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        let textIndex = RuneLargeTextIndex(text: text)

        guard !trimmedQuery.isEmpty else {
            return ResourceLogSearchResult(
                originalText: text,
                displayedText: text,
                query: "",
                matchCase: matchCase,
                totalLineCount: textIndex.lineCount,
                matchingLineCount: textIndex.lineCount,
                matchRanges: [],
                matchLineNumbers: [],
                textIndex: textIndex
            )
        }

        let options: NSString.CompareOptions = matchCase ? [] : [.caseInsensitive, .diacriticInsensitive]
        let searchResult = textIndex.search(query: trimmedQuery, options: options)
        let matchRanges = searchResult.matches.map(\.range)
        let matchLineNumbers = searchResult.matches.map(\.lineNumber)

        return ResourceLogSearchResult(
            originalText: text,
            displayedText: text,
            query: trimmedQuery,
            matchCase: matchCase,
            totalLineCount: searchResult.totalLineCount,
            matchingLineCount: searchResult.matchingLineCount,
            matchRanges: matchRanges,
            matchLineNumbers: matchLineNumbers,
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
        guard !matchLineNumbers.isEmpty else { return nil }
        return matchLineNumbers[clampedMatchIndex(selectedIndex)]
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
        guard !matchRanges.isEmpty else { return nil }
        return sequence &* 1_000_003 &+ clampedMatchIndex(selectedIndex)
    }

    static func makeForInspector(text: String, query: String, matchCase: Bool = false) -> ResourceLogSearchResult {
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        return make(text: text, query: trimmedQuery, matchCase: matchCase)
    }
}

private struct ResourceLogsLoadingPlaceholder: View {
    var body: some View {
        HStack(spacing: 10) {
            ProgressView()
                .controlSize(.small)
            Text("Loading logs…")
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .padding(10)
    }
}

private struct ResourceLogsErrorView: View {
    let message: String
    let onReload: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Could not load logs")
                .font(.body.weight(.semibold))
            Text(message)
                .font(.system(size: 12, weight: .regular, design: .monospaced))
                .foregroundStyle(.red)
                .textSelection(.enabled)
                .fixedSize(horizontal: false, vertical: true)
            Button("Retry", action: onReload)
                .buttonStyle(.borderedProminent)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .padding(10)
    }
}

private struct ResourceLogsEmptyPlaceholder: View {
    let title: String
    let message: String

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(title)
                .font(.body.weight(.medium))
                .foregroundStyle(.secondary)
            Text(message)
                .font(.footnote)
                .foregroundStyle(.tertiary)
                .fixedSize(horizontal: false, vertical: true)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .padding(10)
    }
}
