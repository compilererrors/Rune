import Foundation
import struct RuneSharedCore.RuneLargeTextIndex
import RuneCore
import SwiftUI

struct ResourceLogsToolbar: View {
    @Binding var selectedLogPreset: PodLogPreset
    @Binding var includePreviousLogs: Bool
    @Binding var selectedContainer: String
    @Binding var isTailModeEnabled: Bool
    @Binding var isStreamPaused: Bool
    @Binding var searchQuery: String
    @Binding var selectedSearchMatchIndex: Int
    let searchPulseID: Int
    let searchSummary: ResourceLogSearchResult?
    let statusText: String
    var podOptions: [PodSummary] = []
    var selectedPodID: Binding<String>? = nil
    var showsContainerPicker: Bool = true
    let containerOptions: [String]
    let onReload: () -> Void
    let onSave: () -> Void
    let onSaveVisibleZip: (String) -> Void
    let onSaveFullZip: () -> Void
    let onSaveAllPodsZip: () -> Void
    let onCopySelection: () -> Void
    let onCopyAll: () -> Void
    let onToggleStreamPause: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 9) {
            LogToolbarScrollRow {
                sourceControls
            }
            .controlSize(.small)

            LogToolbarScrollRow {
                modeControls
                toolbarActions
            }
            .controlSize(.small)

            ResourceLogsStatusPanel(
                statusText: statusText,
                showsPreviousHint: includePreviousLogs
            )

            ResourceLogsSearchBar(
                query: $searchQuery,
                selectedMatchIndex: $selectedSearchMatchIndex,
                searchPulseID: searchPulseID,
                searchSummary: searchSummary
            )
        }
    }

    private var sourceControls: some View {
        LogToolbarGroup {
            LogToolbarPickerField(title: "Window") {
                Picker("Log window", selection: $selectedLogPreset) {
                    ForEach(PodLogPreset.allCases) { preset in
                        Text(preset.title).tag(preset)
                    }
                }
                .labelsHidden()
                .frame(width: 158)
            }

            if let selectedPodID, !podOptions.isEmpty {
                LogToolbarPickerField(title: "Pod") {
                    Picker("Pod", selection: selectedPodID) {
                        ForEach(podOptions) { pod in
                            Text(pod.name).tag(pod.id)
                        }
                    }
                    .labelsHidden()
                    .frame(width: 240)
                    .help("Choose a pod in the current namespace for terminal logs.")
                }
            }

            if showsContainerPicker, !containerOptions.isEmpty {
                LogToolbarPickerField(title: "Container") {
                    Picker("Container", selection: $selectedContainer) {
                        Text("All containers").tag("")
                        ForEach(containerOptions, id: \.self) { container in
                            Text(container).tag(container)
                        }
                    }
                    .labelsHidden()
                    .frame(width: 166)
                    .help("Choose one container for pod logs, or keep all containers merged.")
                }
            }
        }
    }

    private var primaryControls: some View {
        HStack(alignment: .center, spacing: 8) {
            sourceControls
            modeControls
        }
        .fixedSize(horizontal: true, vertical: false)
    }

    private var modeControls: some View {
        LogToolbarGroup {
            Toggle("Previous", isOn: $includePreviousLogs)
                .toggleStyle(.button)
                .logToolbarButtonFrame()
                .help("Request logs from the previous container instance when Kubernetes has one.")

            Toggle("Tail", isOn: $isTailModeEnabled)
                .toggleStyle(.button)
                .logToolbarButtonFrame()
                .help("Keep reloading logs and append each read to the session cache.")

            if isTailModeEnabled {
                Button(isStreamPaused ? "Resume" : "Pause", action: onToggleStreamPause)
                    .buttonStyle(.bordered)
                    .logToolbarButtonFrame(width: 74)
                    .help(isStreamPaused ? "Resume live log refresh." : "Pause live log refresh without leaving tail mode.")
            }
        }
    }

    private var toolbarActions: some View {
        LogToolbarGroup {
            Button(action: onReload) {
                Label("Reload", systemImage: "arrow.clockwise")
            }
            .buttonStyle(.bordered)
            .logToolbarButtonFrame()
            .help("Reload logs")

            Button(action: onSave) {
                Label("Save Logs", systemImage: "square.and.arrow.down")
            }
            .buttonStyle(.borderedProminent)
            .logToolbarButtonFrame(width: 114)
            .help("Save current logs")

            Menu {
                Button(action: onCopySelection) {
                    Label("Copy Selection", systemImage: "doc.on.doc")
                }
                Button(action: onCopyAll) {
                    Label("Copy All", systemImage: "doc.on.clipboard")
                }

                Divider()

                Button {
                    onSaveVisibleZip(searchSummary?.displayedText ?? "")
                } label: {
                    Label("Export Visible Results ZIP", systemImage: "doc.zipper")
                }
                Button(action: onSaveFullZip) {
                    Label("Export Full Unfiltered ZIP", systemImage: "archivebox")
                }
                Button(action: onSaveAllPodsZip) {
                    Label("Export All Pods Full ZIP", systemImage: "shippingbox")
                }
            } label: {
                Label("More", systemImage: "ellipsis.circle")
            }
            .buttonStyle(.bordered)
            .logToolbarButtonFrame(width: 96)
            .help("Copy and export log output")
        }
        .fixedSize(horizontal: true, vertical: false)
    }
}

private extension View {
    func logToolbarButtonFrame(width: CGFloat? = nil) -> some View {
        frame(minWidth: width, minHeight: 30, alignment: .center)
            .fixedSize(horizontal: true, vertical: false)
    }
}

private struct LogToolbarGroup<Content: View>: View {
    @ViewBuilder let content: Content

    var body: some View {
        HStack(alignment: .center, spacing: 10) {
            content
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 8)
        .frame(minHeight: 48)
        .background {
            RoundedRectangle(cornerRadius: 9, style: .continuous)
                .fill(Color.primary.opacity(0.055))
        }
        .overlay {
            RoundedRectangle(cornerRadius: 9, style: .continuous)
                .strokeBorder(Color(nsColor: .separatorColor).opacity(0.16), lineWidth: 1)
        }
        .fixedSize(horizontal: true, vertical: false)
    }
}

private struct LogToolbarScrollRow<Content: View>: View {
    @ViewBuilder let content: Content

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(alignment: .center, spacing: 8) {
                content
            }
            .fixedSize(horizontal: true, vertical: false)
        }
    }
}

private struct LogToolbarPickerField<Content: View>: View {
    let title: String
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
        switch title {
        case "Pod":
            return 240
        case "Container":
            return 166
        default:
            return 158
        }
    }

}

private struct ResourceLogsStatusPanel: View {
    let statusText: String
    let showsPreviousHint: Bool

    var body: some View {
        HStack(alignment: .center, spacing: 8) {
            HStack(alignment: .center, spacing: 7) {
                Circle()
                    .fill(statusColor)
                    .frame(width: 7, height: 7)

                Text(statusText)
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.primary)
                    .lineLimit(1)
                    .truncationMode(.middle)
                    .monospacedDigit()
            }
            .padding(.horizontal, 9)
            .padding(.vertical, 5)
            .background {
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .fill(Color.primary.opacity(0.035))
            }
            .overlay {
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .strokeBorder(Color(nsColor: .separatorColor).opacity(0.14), lineWidth: 1)
            }

            if showsPreviousHint {
                Text("Previous logs only exist for restarted containers.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .truncationMode(.tail)
            }
        }
        .fixedSize(horizontal: false, vertical: true)
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
    var showsContainerPicker: Bool = true
    let containerOptions: [String]
    let logText: String
    let readOnlyResetID: String
    let onReload: () -> Void
    let onSave: () -> Void
    let onSaveVisibleZip: (String) -> Void
    let onSaveFullZip: () -> Void
    let onSaveAllPodsZip: () -> Void
    let onCopySelection: () -> Void
    let onCopyAll: () -> Void
    let onToggleStreamPause: () -> Void
    @State private var searchQuery = ""
    @State private var selectedSearchMatchIndex = 0
    @State private var searchNavigationSequence = 0
    @State private var searchPulseID = 0
    @State private var structuredLogSummary = ResourceStructuredLogAnalyzer.analyze(text: "")

    var body: some View {
        let searchResult = ResourceLogSearchResult.makeForInspector(text: logText, query: searchQuery)
        let resolvedSearchMatchIndex = searchResult.clampedMatchIndex(selectedSearchMatchIndex)

        VStack(alignment: .leading, spacing: 10) {
            ResourceLogsToolbar(
                selectedLogPreset: $selectedLogPreset,
                includePreviousLogs: $includePreviousLogs,
                selectedContainer: $selectedContainer,
                isTailModeEnabled: $isTailModeEnabled,
                isStreamPaused: $isStreamPaused,
                searchQuery: $searchQuery,
                selectedSearchMatchIndex: $selectedSearchMatchIndex,
                searchPulseID: searchPulseID,
                searchSummary: searchResult,
                statusText: statusText,
                podOptions: podOptions,
                selectedPodID: selectedPodID,
                showsContainerPicker: showsContainerPicker,
                containerOptions: containerOptions,
                onReload: onReload,
                onSave: onSave,
                onSaveVisibleZip: onSaveVisibleZip,
                onSaveFullZip: onSaveFullZip,
                onSaveAllPodsZip: onSaveAllPodsZip,
                onCopySelection: onCopySelection,
                onCopyAll: onCopyAll,
                onToggleStreamPause: onToggleStreamPause
            )

            ResourceStructuredLogSummaryPanel(
                summary: structuredLogSummary,
                onSearchFieldSample: { field, value in
                    applyLogSearchQuery(ResourceStructuredLogFieldSearch.query(field: field, value: value))
                },
                onSearchDuplicate: { duplicate in
                    applyLogSearchQuery(duplicate.fingerprint)
                }
            )

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
        .onChange(of: logText) { _, _ in
            selectedSearchMatchIndex = searchResult.clampedMatchIndex(selectedSearchMatchIndex)
        }
        .task(id: logText) {
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
    let onSaveVisibleZip: (String) -> Void
    let onSaveFullZip: () -> Void
    let onSaveAllPodsZip: () -> Void
    let onCopySelection: () -> Void
    let onCopyAll: () -> Void
    let onToggleStreamPause: () -> Void
    @State private var searchQuery = ""
    @State private var selectedSearchMatchIndex = 0
    @State private var searchNavigationSequence = 0
    @State private var searchPulseID = 0
    @State private var structuredLogSummary = ResourceStructuredLogAnalyzer.analyze(text: "")

    var body: some View {
        let searchResult = ResourceLogSearchResult.makeForInspector(text: logText, query: searchQuery)
        let resolvedSearchMatchIndex = searchResult.clampedMatchIndex(selectedSearchMatchIndex)

        VStack(alignment: .leading, spacing: 10) {
            ResourceLogsToolbar(
                selectedLogPreset: $selectedLogPreset,
                includePreviousLogs: $includePreviousLogs,
                selectedContainer: .constant(""),
                isTailModeEnabled: $isTailModeEnabled,
                isStreamPaused: $isStreamPaused,
                searchQuery: $searchQuery,
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
                onSaveVisibleZip: onSaveVisibleZip,
                onSaveFullZip: onSaveFullZip,
                onSaveAllPodsZip: onSaveAllPodsZip,
                onCopySelection: onCopySelection,
                onCopyAll: onCopyAll,
                onToggleStreamPause: onToggleStreamPause
            )

            if !podNames.isEmpty {
                ResourceLogsSourcePanel(title: "Pods", values: podNames)
            }

            ResourceStructuredLogSummaryPanel(
                summary: structuredLogSummary,
                onSearchFieldSample: { field, value in
                    applyLogSearchQuery(ResourceStructuredLogFieldSearch.query(field: field, value: value))
                },
                onSearchDuplicate: { duplicate in
                    applyLogSearchQuery(duplicate.fingerprint)
                }
            )

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
        .onChange(of: logText) { _, _ in
            selectedSearchMatchIndex = searchResult.clampedMatchIndex(selectedSearchMatchIndex)
        }
        .task(id: logText) {
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
    @Binding var selectedMatchIndex: Int
    let searchPulseID: Int
    let searchSummary: ResourceLogSearchResult?
    @State private var draftQuery = ""
    @State private var queryCommitTask: Task<Void, Never>?
    @State private var pulseOpacity = 0.0

    var body: some View {
        HStack(spacing: 10) {
            HStack(spacing: 8) {
                Image(systemName: "magnifyingglass")
                    .foregroundStyle(.secondary)
                TextField("Search logs", text: $draftQuery)
                    .textFieldStyle(.plain)
                    .font(.system(size: 12, weight: .regular, design: .monospaced))
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
                HStack(spacing: 8) {
                    Button {
                        selectedMatchIndex = searchSummary.previousMatchIndex(from: selectedMatchIndex)
                    } label: {
                        Image(systemName: "chevron.up")
                            .font(.system(size: 12, weight: .medium))
                            .foregroundStyle(.secondary)
                            .padding(.horizontal, 5)
                            .padding(.vertical, 3)
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
                            .padding(.horizontal, 5)
                            .padding(.vertical, 3)
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
                    .padding(.horizontal, 10)
                    .padding(.vertical, 6)
                    .background {
                        Capsule(style: .continuous)
                            .fill(Color.accentColor.opacity(searchSummary.matchingLineCount == 0 ? 0.08 : 0.16))
                    }

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
                    .padding(.horizontal, 9)
                    .padding(.vertical, 6)
                    .background {
                        Capsule(style: .continuous)
                            .fill(Color.primary.opacity(searchSummary.hasMatches ? 0.06 : 0))
                    }
                    .accessibilityLabel(
                        searchSummary.hasMatches
                            ? "Search match \(searchSummary.matchPositionText(selectedIndex: selectedMatchIndex))"
                            : "Search match position"
                    )
                }
                .fixedSize(horizontal: true, vertical: false)
            }
        }
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
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        let textIndex = RuneLargeTextIndex(text: text)

        guard !trimmedQuery.isEmpty else {
            return ResourceLogSearchResult(
                originalText: text,
                displayedText: text,
                query: "",
                totalLineCount: textIndex.lineCount,
                matchingLineCount: textIndex.lineCount,
                matchRanges: [],
                matchLineNumbers: [],
                textIndex: textIndex
            )
        }

        let searchResult = textIndex.search(query: trimmedQuery)
        let matchRanges = searchResult.matches.map(\.range)
        let matchLineNumbers = searchResult.matches.map(\.lineNumber)

        return ResourceLogSearchResult(
            originalText: text,
            displayedText: text,
            query: trimmedQuery,
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

    static func makeForInspector(text: String, query: String) -> ResourceLogSearchResult {
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        return make(text: text, query: trimmedQuery)
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
