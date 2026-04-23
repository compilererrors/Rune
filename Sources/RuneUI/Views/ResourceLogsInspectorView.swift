import SwiftUI

struct ResourceLogsToolbar: View {
    @Binding var selectedLogPreset: PodLogPreset
    @Binding var includePreviousLogs: Bool
    let onReload: () -> Void
    let onSave: () -> Void

    var body: some View {
        HStack {
            Picker("Log window", selection: $selectedLogPreset) {
                ForEach(PodLogPreset.allCases) { preset in
                    Text(preset.title).tag(preset)
                }
            }
            .frame(maxWidth: 220)

            Toggle("Previous", isOn: $includePreviousLogs)

            Spacer()

            Button("Reload", action: onReload)
            Button("Save Logs", action: onSave)
        }
    }
}

struct PodLogsInspectorPane: View {
    @Binding var selectedLogPreset: PodLogPreset
    @Binding var includePreviousLogs: Bool
    let isLoadingLogs: Bool
    let isLoadingResources: Bool
    let errorMessage: String?
    let logText: String
    let readOnlyResetID: String
    let onReload: () -> Void
    let onSave: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            ResourceLogsToolbar(
                selectedLogPreset: $selectedLogPreset,
                includePreviousLogs: $includePreviousLogs,
                onReload: onReload,
                onSave: onSave
            )

            ResourceLogsOutputSurface(
                isLoadingLogs: isLoadingLogs,
                isLoadingResources: isLoadingResources,
                errorMessage: errorMessage,
                logText: logText,
                emptyTitle: "No log output",
                emptyMessage: "The pod may be idle, or the current filter returned no lines.",
                readOnlyResetID: readOnlyResetID,
                onReload: onReload
            )
        }
    }
}

struct UnifiedResourceLogsInspectorPane: View {
    @Binding var selectedLogPreset: PodLogPreset
    @Binding var includePreviousLogs: Bool
    let isLoadingLogs: Bool
    let isLoadingResources: Bool
    let errorMessage: String?
    let podNames: [String]
    let logText: String
    let readOnlyResetID: String
    let onReload: () -> Void
    let onSave: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            ResourceLogsToolbar(
                selectedLogPreset: $selectedLogPreset,
                includePreviousLogs: $includePreviousLogs,
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
                logText: logText,
                emptyTitle: "No log output",
                emptyMessage: "No lines were returned for the selected pods and the current filter. Pods may be idle or produce no output for this time window.",
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
    let logText: String
    let emptyTitle: String
    let emptyMessage: String
    let readOnlyResetID: String
    let onReload: () -> Void

    var body: some View {
        InspectorTextSurface(minHeight: 280) {
            Group {
                if isLoadingLogs || isLoadingResources {
                    ResourceLogsLoadingPlaceholder()
                } else if let errorMessage {
                    ResourceLogsErrorView(message: errorMessage, onReload: onReload)
                } else if logText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    ResourceLogsEmptyPlaceholder(title: emptyTitle, message: emptyMessage)
                } else {
                    InspectorReadOnlyTextView(
                        text: logText,
                        resetID: readOnlyResetID
                    )
                }
            }
        }
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
