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

                        InspectorReadOnlyTextView(
                            text: searchResult.displayedText,
                            resetID: "\(readOnlyResetID):\(searchResult.resetToken)"
                        )
                    }
                }
            }
        }
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

    var resetToken: String {
        "\(query)|\(matchingLineCount)|\(displayedText.count)"
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
        let lines = text.split(omittingEmptySubsequences: false, whereSeparator: \.isNewline).map(String.init)

        guard !trimmedQuery.isEmpty else {
            return ResourceLogSearchResult(
                originalText: text,
                displayedText: text,
                query: "",
                totalLineCount: lines.count,
                matchingLineCount: lines.count
            )
        }

        let matching = lines.filter {
            $0.range(of: trimmedQuery, options: NSString.CompareOptions([.caseInsensitive, .diacriticInsensitive])) != nil
        }

        return ResourceLogSearchResult(
            originalText: text,
            displayedText: matching.joined(separator: "\n"),
            query: trimmedQuery,
            totalLineCount: lines.count,
            matchingLineCount: matching.count
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
