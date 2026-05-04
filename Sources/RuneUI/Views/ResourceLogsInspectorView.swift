import Foundation
import SwiftUI

struct ResourceLogsToolbar: View {
    @Binding var selectedLogPreset: PodLogPreset
    @Binding var includePreviousLogs: Bool
    @Binding var isTailModeEnabled: Bool
    @Binding var searchQuery: String
    let searchSummary: ResourceLogSearchResult?
    let onReload: () -> Void
    let onSave: () -> Void

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

                Toggle("Tail", isOn: $isTailModeEnabled)
                    .help("Keep reloading logs and append each read to the session cache.")

                Spacer()

                Button("Reload", action: onReload)
                Button("Save Logs", action: onSave)
            }

            ResourceLogsSearchBar(
                query: $searchQuery,
                searchSummary: searchSummary
            )
        }
    }
}

struct PodLogsInspectorPane: View {
    @Binding var selectedLogPreset: PodLogPreset
    @Binding var includePreviousLogs: Bool
    @Binding var isTailModeEnabled: Bool
    let isLoadingLogs: Bool
    let isLoadingResources: Bool
    let errorMessage: String?
    let logText: String
    let readOnlyResetID: String
    let onReload: () -> Void
    let onSave: () -> Void
    @State private var searchQuery = ""

    var body: some View {
        let searchResult = ResourceLogSearchResult.make(text: logText, query: searchQuery)

        VStack(alignment: .leading, spacing: 10) {
            ResourceLogsToolbar(
                selectedLogPreset: $selectedLogPreset,
                includePreviousLogs: $includePreviousLogs,
                isTailModeEnabled: $isTailModeEnabled,
                searchQuery: $searchQuery,
                searchSummary: searchResult,
                onReload: onReload,
                onSave: onSave
            )

            ResourceLogsOutputSurface(
                isLoadingLogs: isLoadingLogs,
                isLoadingResources: isLoadingResources,
                errorMessage: errorMessage,
                searchResult: searchResult,
                emptyTitle: "No log output",
                emptyMessage: "The pod may be idle, or the current filter returned no lines.",
                noMatchesMessage: "No log lines matched the current search.",
                readOnlyResetID: readOnlyResetID,
                onReload: onReload
            )
        }
    }
}

struct UnifiedResourceLogsInspectorPane: View {
    @Binding var selectedLogPreset: PodLogPreset
    @Binding var includePreviousLogs: Bool
    @Binding var isTailModeEnabled: Bool
    let isLoadingLogs: Bool
    let isLoadingResources: Bool
    let errorMessage: String?
    let podNames: [String]
    let logText: String
    let readOnlyResetID: String
    let onReload: () -> Void
    let onSave: () -> Void
    @State private var searchQuery = ""

    var body: some View {
        let searchResult = ResourceLogSearchResult.make(text: logText, query: searchQuery)

        VStack(alignment: .leading, spacing: 10) {
            ResourceLogsToolbar(
                selectedLogPreset: $selectedLogPreset,
                includePreviousLogs: $includePreviousLogs,
                isTailModeEnabled: $isTailModeEnabled,
                searchQuery: $searchQuery,
                searchSummary: searchResult,
                onReload: onReload,
                onSave: onSave
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
                emptyTitle: "No log output",
                emptyMessage: "No lines were returned for the selected pods and the current filter. Pods may be idle or produce no output for this time window.",
                noMatchesMessage: "No unified log lines matched the current search.",
                readOnlyResetID: readOnlyResetID,
                onReload: onReload
            )
        }
    }
}

private struct ResourceLogsOutputSurface: View {
    let isLoadingLogs: Bool
    let isLoadingResources: Bool
    let errorMessage: String?
    let searchResult: ResourceLogSearchResult
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
                } else if searchResult.isFiltering, searchResult.displayedText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    ResourceLogsEmptyPlaceholder(title: "No search matches", message: noMatchesMessage)
                } else {
                    VStack(alignment: .leading, spacing: 0) {
                        if searchResult.isFiltering {
                            ResourceLogsSearchSummaryBar(searchResult: searchResult)
                        }

                        let outputResetID = "\(readOnlyResetID):\(searchResult.scrollIdentityToken)"
                        if shouldDeferOutputMount {
                            DeferredResourceLogsTextView(
                                text: searchResult.displayedText,
                                resetID: outputResetID
                            )
                        } else {
                            InspectorReadOnlyTextView(
                                text: searchResult.displayedText,
                                resetID: outputResetID,
                                resetScrollOnExternalChange: false
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

    static func shouldDeferOutputMount(for searchResult: ResourceLogSearchResult) -> Bool {
        !searchResult.isFiltering && searchResult.displayedText.utf8.count > deferredOutputThreshold
    }
}

private struct DeferredResourceLogsTextView: View {
    let text: String
    let resetID: String
    @State private var isReady = false
    @State private var renderTask: Task<Void, Never>?

    var body: some View {
        Group {
            if isReady {
                InspectorReadOnlyTextView(
                    text: text,
                    resetID: resetID,
                    resetScrollOnExternalChange: false
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

    var isFiltering: Bool {
        !query.isEmpty
    }

    var scrollIdentityToken: String {
        "query:\(query)"
    }

    var badgeText: String {
        matchingLineCount == 1 ? "1 match" : "\(matchingLineCount) matches"
    }

    var summaryText: String {
        if matchingLineCount == 0 {
            return "No matching lines in \(totalLineCount) total lines."
        }
        return "Showing \(matchingLineCount) matching lines out of \(totalLineCount)."
    }

    static func make(text: String, query: String) -> ResourceLogSearchResult {
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)

        guard !trimmedQuery.isEmpty else {
            let lineCount = lineCount(in: text)
            return ResourceLogSearchResult(
                originalText: text,
                displayedText: text,
                query: "",
                totalLineCount: lineCount,
                matchingLineCount: lineCount
            )
        }

        let isASCIIQuery = trimmedQuery.unicodeScalars.allSatisfy(\.isASCII)
        if isASCIIQuery, text.utf8.allSatisfy({ $0 != 13 }) {
            return makeFastASCIIFilteredResult(text: text, query: trimmedQuery)
        }

        let options = NSString.CompareOptions([.caseInsensitive, .diacriticInsensitive])
        var totalLineCount = 0
        var matchingLineCount = 0
        var displayedText = ""

        for line in text.split(omittingEmptySubsequences: false, whereSeparator: \.isNewline) {
            totalLineCount += 1
            let isMatch = line.range(of: trimmedQuery, options: options) != nil

            if isMatch {
                if matchingLineCount > 0 {
                    displayedText.append("\n")
                }
                displayedText.append(contentsOf: line)
                matchingLineCount += 1
            }
        }

        return ResourceLogSearchResult(
            originalText: text,
            displayedText: displayedText,
            query: trimmedQuery,
            totalLineCount: totalLineCount,
            matchingLineCount: matchingLineCount
        )
    }

    private static func makeFastASCIIFilteredResult(text: String, query: String) -> ResourceLogSearchResult {
        let nsText = text as NSString
        let textLength = nsText.length
        let totalLineCount = lineCount(in: text)
        var matchingLineCount = 0
        var displayedText = ""
        displayedText.reserveCapacity(min(text.utf8.count, 1_048_576))

        var searchLocation = 0
        while searchLocation < textLength {
            let searchRange = NSRange(location: searchLocation, length: textLength - searchLocation)
            let matchRange = nsText.range(
                of: query,
                options: [.caseInsensitive],
                range: searchRange
            )
            guard matchRange.location != NSNotFound else { break }

            let beforeMatch = NSRange(location: 0, length: matchRange.location)
            let previousLineBreak = nsText.range(
                of: "\n",
                options: [.backwards],
                range: beforeMatch
            )
            let lineStart = previousLineBreak.location == NSNotFound
                ? 0
                : previousLineBreak.location + previousLineBreak.length

            let afterMatch = NSRange(
                location: matchRange.location,
                length: textLength - matchRange.location
            )
            let nextLineBreak = nsText.range(of: "\n", range: afterMatch)
            let lineEnd = nextLineBreak.location == NSNotFound ? textLength : nextLineBreak.location
            let nextSearchLocation = nextLineBreak.location == NSNotFound
                ? textLength
                : nextLineBreak.location + nextLineBreak.length

            if matchingLineCount > 0 {
                displayedText.append("\n")
            }
            displayedText.append(nsText.substring(with: NSRange(location: lineStart, length: lineEnd - lineStart)))
            matchingLineCount += 1
            searchLocation = nextSearchLocation
        }

        return ResourceLogSearchResult(
            originalText: text,
            displayedText: displayedText,
            query: query,
            totalLineCount: totalLineCount,
            matchingLineCount: matchingLineCount
        )
    }

    private static func lineCount(in text: String) -> Int {
        guard !text.isEmpty else { return 0 }
        guard text.utf8.allSatisfy({ $0 < 128 }) else {
            return text.reduce(1) { count, character in
                character.isNewline ? count + 1 : count
            }
        }

        var count = 1
        var previousWasCarriageReturn = false
        for byte in text.utf8 {
            switch byte {
            case 10:
                if previousWasCarriageReturn {
                    previousWasCarriageReturn = false
                } else {
                    count += 1
                }
            case 13:
                count += 1
                previousWasCarriageReturn = true
            default:
                previousWasCarriageReturn = false
            }
        }

        return count
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
