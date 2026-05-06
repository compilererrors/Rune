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
    let searchSummary: ResourceLogSearchResult?
    let statusText: String
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
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Picker("Log window", selection: $selectedLogPreset) {
                    ForEach(PodLogPreset.allCases) { preset in
                        Text(preset.title).tag(preset)
                    }
                }
                .frame(maxWidth: 220)

                Toggle("Previous", isOn: $includePreviousLogs)

                if !containerOptions.isEmpty {
                    Picker("Container", selection: $selectedContainer) {
                        Text("All containers").tag("")
                        ForEach(containerOptions, id: \.self) { container in
                            Text(container).tag(container)
                        }
                    }
                    .frame(maxWidth: 180)
                    .help("Choose one container for pod logs, or keep all containers merged.")
                }

                Toggle("Tail", isOn: $isTailModeEnabled)
                    .help("Keep reloading logs and append each read to the session cache.")

                if isTailModeEnabled {
                    Button(isStreamPaused ? "Resume" : "Pause", action: onToggleStreamPause)
                        .help(isStreamPaused ? "Resume live log refresh." : "Pause live log refresh without leaving tail mode.")
                }

                Spacer()

                Button("Reload", action: onReload)
                Button("Save Logs", action: onSave)
                Menu("Export ZIP") {
                    Button("Visible Results ZIP") {
                        onSaveVisibleZip(searchSummary?.displayedText ?? "")
                    }
                    Button("Full Unfiltered ZIP", action: onSaveFullZip)
                    Button("All Pods Full ZIP", action: onSaveAllPodsZip)
                }
                Button("Copy Selection", action: onCopySelection)
                Button("Copy All", action: onCopyAll)
            }

            HStack(spacing: 6) {
                Circle()
                    .fill(statusText.lowercased().contains("failed") ? Color.red : (statusText.lowercased().contains("loading") ? Color.blue : Color.green))
                    .frame(width: 7, height: 7)
                Text(statusText)
                    .font(.caption.weight(.medium))
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }

            if includePreviousLogs {
                Label("Previous logs are returned only for restarted containers; empty previous output can be normal.", systemImage: "info.circle")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(2)
            }

            ResourceLogsSearchBar(
                query: $searchQuery,
                selectedMatchIndex: $selectedSearchMatchIndex,
                searchSummary: searchSummary
            )
        }
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
                searchSummary: searchResult,
                statusText: statusText,
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
                searchSummary: searchResult,
                statusText: statusText,
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
                Text("Pods: " + podNames.joined(separator: ", "))
                    .font(.footnote.weight(.medium))
                    .foregroundStyle(.secondary)
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
        .onChange(of: logText) { _, _ in
            selectedSearchMatchIndex = searchResult.clampedMatchIndex(selectedSearchMatchIndex)
        }
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
                            ResourceLogsSearchSummaryBar(searchResult: searchResult)
                        }

                        let outputResetID = "\(readOnlyResetID):\(searchResult.scrollIdentityToken)"
                        let navigationRequest = searchResult.navigationRequest(
                            selectedIndex: selectedSearchMatchIndex,
                            sequence: searchNavigationSequence
                        )
                        if shouldDeferOutputMount {
                            DeferredResourceLogsTextView(
                                text: searchResult.displayedText,
                                resetID: outputResetID,
                                navigationRequest: navigationRequest
                            )
                        } else {
                            InspectorReadOnlyTextView(
                                text: searchResult.displayedText,
                                resetID: outputResetID,
                                resetScrollOnExternalChange: false,
                                contentStyle: .ansiLogs,
                                navigationRequest: navigationRequest
                            )
                        }
                    }
                }
            }
        }
    }

    private var shouldDeferOutputMount: Bool {
        ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: searchResult)
    }
}

enum ResourceLogsDeferredRenderingPolicy {
    static let deferredOutputThreshold = 250_000

    static func shouldDeferOutputMount(text: String, query: String) -> Bool {
        query.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty && text.utf8.count > deferredOutputThreshold
    }

    static func shouldDeferOutputMount(for searchResult: ResourceLogSearchResult) -> Bool {
        searchResult.displayedText.utf8.count > deferredOutputThreshold
    }
}

private struct DeferredResourceLogsTextView: View {
    let text: String
    let resetID: String
    let navigationRequest: YAMLTextNavigationRequest?
    @State private var isReady = false
    @State private var renderTask: Task<Void, Never>?

    var body: some View {
        Group {
            if isReady {
                InspectorReadOnlyTextView(
                    text: text,
                    resetID: resetID,
                    resetScrollOnExternalChange: false,
                    contentStyle: .ansiLogs,
                    navigationRequest: navigationRequest
                )
            } else {
                ResourceLogsPreparingPlaceholder()
            }
        }
        .onAppear {
            scheduleRender()
        }
        .onChange(of: resetID) {
            scheduleRender()
        }
        .onDisappear {
            renderTask?.cancel()
            renderTask = nil
            isReady = false
        }
    }

    private func scheduleRender() {
        renderTask?.cancel()
        isReady = false
        let expectedResetID = resetID
        renderTask = Task {
            try? await Task.sleep(nanoseconds: 500_000_000)
            guard !Task.isCancelled else { return }
            await MainActor.run {
                guard expectedResetID == resetID else { return }
                isReady = true
            }
        }
    }
}

private struct ResourceLogsPreparingPlaceholder: View {
    var body: some View {
        HStack(spacing: 10) {
            ProgressView()
                .controlSize(.small)
            Text("Preparing log output…")
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .padding(10)
    }
}

private struct ResourceLogsSearchBar: View {
    @Binding var query: String
    @Binding var selectedMatchIndex: Int
    let searchSummary: ResourceLogSearchResult?

    var body: some View {
        HStack(spacing: 10) {
            HStack(spacing: 8) {
                Image(systemName: "magnifyingglass")
                    .foregroundStyle(.secondary)
                TextField("Search logs", text: $query)
                    .textFieldStyle(.plain)
                    .font(.system(size: 12, weight: .regular, design: .monospaced))
                if !query.isEmpty {
                    Button {
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

            if let searchSummary, searchSummary.isFiltering {
                Button {
                    selectedMatchIndex = searchSummary.previousMatchIndex(from: selectedMatchIndex)
                } label: {
                    Image(systemName: "chevron.up")
                }
                .buttonStyle(.plain)
                .disabled(searchSummary.matchRanges.isEmpty)
                .help("Previous match")

                Button {
                    selectedMatchIndex = searchSummary.nextMatchIndex(from: selectedMatchIndex)
                } label: {
                    Image(systemName: "chevron.down")
                }
                .buttonStyle(.plain)
                .disabled(searchSummary.matchRanges.isEmpty)
                .help("Next match")

                Text(searchSummary.badgeText)
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(searchSummary.matchingLineCount == 0 ? .secondary : .primary)
                    .padding(.horizontal, 10)
                    .padding(.vertical, 6)
                    .background {
                        Capsule(style: .continuous)
                            .fill(Color.accentColor.opacity(searchSummary.matchingLineCount == 0 ? 0.08 : 0.16))
                    }
            }
        }
    }
}

private struct ResourceLogsSearchSummaryBar: View {
    let searchResult: ResourceLogSearchResult

    var body: some View {
        HStack {
            Text(searchResult.summaryText)
                .font(.footnote.weight(.medium))
                .foregroundStyle(.secondary)
            Spacer()
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

    var isFiltering: Bool {
        !query.isEmpty
    }

    var scrollIdentityToken: String {
        "logs"
    }

    var badgeText: String {
        matchingLineCount == 1 ? "1 match" : "\(matchingLineCount) matches"
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
                matchRanges: []
            )
        }

        let searchResult = textIndex.search(query: trimmedQuery)
        let matchRanges = searchResult.matches.map(\.range)

        return ResourceLogSearchResult(
            originalText: text,
            displayedText: text,
            query: trimmedQuery,
            totalLineCount: searchResult.totalLineCount,
            matchingLineCount: searchResult.matchingLineCount,
            matchRanges: matchRanges
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

    static func makeForInspector(text: String, query: String) -> ResourceLogSearchResult {
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(text: text, query: trimmedQuery) else {
            return make(text: text, query: trimmedQuery)
        }
        return ResourceLogSearchResult(
            originalText: text,
            displayedText: text,
            query: "",
            totalLineCount: 0,
            matchingLineCount: 0,
            matchRanges: []
        )
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
