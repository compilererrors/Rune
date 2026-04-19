import AppKit
import RuneCore
import RuneDiagnostics
import SwiftUI

/// Gear-menu preferences: cache, diagnostics, and verbose file trace (`debug-trace.log`).
public struct RunePreferencesView: View {
    @Environment(\.dismiss) private var dismiss
    @AppStorage(RuneSettingsKeys.persistNamespaceListCache) private var persistNamespaceListCache = true
    @AppStorage(RuneSettingsKeys.diagnosticsLogging) private var diagnosticsLogging = true
    @AppStorage(RuneSettingsKeys.verboseDebugTrace) private var verboseDebugTrace = false
    @AppStorage(RuneSettingsKeys.backgroundPrefetchOtherContexts) private var backgroundPrefetchOtherContexts = false

    public init() {}

    public var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            HStack(alignment: .center, spacing: 12) {
                Button {
                    dismiss()
                } label: {
                    Image(systemName: "chevron.backward")
                        .font(.body.weight(.semibold))
                        .frame(width: 28, height: 28)
                }
                .buttonStyle(.bordered)
                .controlSize(.small)
                .help("Close settings")

                VStack(alignment: .leading, spacing: 2) {
                    Text("Settings")
                        .font(.title3.weight(.semibold))
                    Text("Caching and diagnostics on this Mac.")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }

                Spacer(minLength: 0)
            }
            .padding(.bottom, 14)

            Form {
                Section {
                    Toggle("Persist namespace lists", isOn: $persistNamespaceListCache)
                    Text(
                        "When on, the last namespace menu per context is saved under Application Support and shown immediately on cold start while a fresh list loads in the background."
                    )
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                } header: {
                    Text("Cache")
                }

                Section {
                    Toggle("Diagnostics logging", isOn: $diagnosticsLogging)
                    Text(
                        "When on, Rune writes diagnostic messages to the system log (NSLog, prefix `[Rune]`) and mirrors kubectl/helm lines to the unified log (subsystem `com.rune.desktop`, category `CommandRunner`)."
                    )
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                } header: {
                    Text("Diagnostics")
                }

                Section {
                    Toggle("Verbose debug trace (file)", isOn: $verboseDebugTrace)
                    Text(
                        "When on, Rune appends timestamped trace lines for Kubernetes loads (snapshots, refresh, resource details, prefetch) to a file under Application Support (`Rune/Logs/debug-trace.log`). From Terminal you can also set RUNE_VERBOSE_DEBUG_TRACE=1 and RUNE_LOG_TO_STDERR=1 when launching the app executable."
                    )
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)

                    VStack(alignment: .leading, spacing: 8) {
                        Text(DebugTraceWriter.logFileURL.path)
                            .font(.caption.monospaced())
                            .foregroundStyle(.secondary)
                            .textSelection(.enabled)

                        HStack(spacing: 10) {
                            Button("Reveal debug trace in Finder") {
                                revealDebugTraceLogInFinder()
                            }
                            .buttonStyle(.bordered)

                            Button("Clear debug trace log", role: .destructive) {
                                DebugTraceWriter.clear()
                            }
                            .buttonStyle(.bordered)
                        }
                    }
                    .padding(.top, 4)
                } header: {
                    Text("Verbose trace")
                }

                Section {
                    Text(
                        "Optional JSONL for kubectl comparisons: set `RUNE_K8S_TRACE_FILE=/path/to/trace.jsonl` before launch (same file as Go `rune-k8s-agent` when comparing backends)."
                    )
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                } header: {
                    Text("Wire trace (JSONL)")
                }

                Section {
                    Text(
                        "Capture Console + stderr while testing from Terminal:"
                    )
                    .font(.footnote)
                    .foregroundStyle(.secondary)

                    Text(
                        "RUNE_VERBOSE_DEBUG_TRACE=1 RUNE_DIAGNOSTICS_LOGGING=1 RUNE_LOG_TO_STDERR=1 \\\n\"/path/to/Rune.app/Contents/MacOS/RuneApp\" 2>&1 | tee ~/Desktop/rune-k8s-debug.log"
                    )
                    .font(.caption.monospaced())
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)
                    .fixedSize(horizontal: false, vertical: true)
                } header: {
                    Text("Terminal / tee")
                }

                Section {
                    Toggle("Background prefetch other contexts", isOn: $backgroundPrefetchOtherContexts)
                    Text(
                        "When on, the app may warm overview cache for a few non-selected contexts in the background (bounded). Requires a future app build that implements the warm pass; the toggle stores the preference now."
                    )
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                } header: {
                    Text("Performance")
                }
            }
            .formStyle(.grouped)
        }
        .padding(16)
        .frame(minWidth: 460, idealWidth: 520, minHeight: 520)
        .background(.thinMaterial)
        .onExitCommand {
            dismiss()
        }
    }

    private func revealDebugTraceLogInFinder() {
        let url = DebugTraceWriter.logFileURL
        let parent = url.deletingLastPathComponent()
        if FileManager.default.fileExists(atPath: url.path) {
            NSWorkspace.shared.activateFileViewerSelecting([url])
        } else if FileManager.default.fileExists(atPath: parent.path) {
            NSWorkspace.shared.open(parent)
        } else {
            try? FileManager.default.createDirectory(at: parent, withIntermediateDirectories: true)
            NSWorkspace.shared.open(parent)
        }
    }
}
