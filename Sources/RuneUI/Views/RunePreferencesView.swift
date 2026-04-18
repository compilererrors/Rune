import RuneCore
import SwiftUI

/// Gear-menu preferences: on-disk namespace cache and diagnostics logging.
public struct RunePreferencesView: View {
    @Environment(\.dismiss) private var dismiss
    @AppStorage(RuneSettingsKeys.persistNamespaceListCache) private var persistNamespaceListCache = true
    @AppStorage(RuneSettingsKeys.diagnosticsLogging) private var diagnosticsLogging = true

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
                    Text("When on, Rune writes diagnostic messages to the system log (NSLog). Turn off to keep the console quiet.")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                } header: {
                    Text("Diagnostics")
                }
            }
            .formStyle(.grouped)
        }
        .padding(16)
        .frame(minWidth: 460, idealWidth: 500, minHeight: 340)
        .background(.thinMaterial)
        .onExitCommand {
            dismiss()
        }
    }
}
