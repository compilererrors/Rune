import SwiftUI

struct AddClusterNativeContextSection: View {
    let options: [AddClusterNativeContextOption]
    @Binding var selectedBindingID: String?
    let connectedBindingIDs: Set<String>
    let isCheckingProfiles: Bool
    let analysisMessage: String?
    @Environment(\.runeThemePalette) private var runeThemePalette

    private var selectedOption: AddClusterNativeContextOption? {
        guard let selectedBindingID else { return nil }
        return options.first { $0.id == selectedBindingID }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 7) {
            Text("Imported context")
                .font(.caption)
                .foregroundStyle(.secondary)

            if options.isEmpty {
                Label {
                    Text(
                        isCheckingProfiles
                            ? "Checking imported contexts…"
                            : analysisMessage
                                ?? "Import a kubeconfig to add this cluster. Rune checks its authentication settings after import."
                    )
                } icon: {
                    if isCheckingProfiles {
                        ProgressView()
                            .controlSize(.small)
                    } else {
                        Image(systemName: "doc.badge.plus")
                    }
                }
                .font(.caption)
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(9)
                .background(RuneSurfaceBackground(kind: .inset))
            } else {
                Picker("Native authentication context", selection: $selectedBindingID) {
                    if selectedBindingID == nil {
                        Text("Choose a compatible context…")
                            .tag(String?.none)
                    }
                    ForEach(options) { option in
                        Text(option.contextName)
                            .tag(Optional(option.id))
                    }
                }
                .pickerStyle(.menu)
                .controlSize(.regular)
                .accessibilityLabel("Native authentication context")

                HStack(spacing: 6) {
                    if isCheckingProfiles {
                        ProgressView()
                            .controlSize(.small)
                        Text("Checking credential profile…")
                    } else if let selectedOption {
                        let isConnected = connectedBindingIDs.contains(selectedOption.id)
                        Image(systemName: isConnected ? "checkmark.circle.fill" : "circle.dashed")
                            .foregroundStyle(
                                isConnected
                                    ? RuneSemanticColorRole.success.color(in: runeThemePalette)
                                    : Color.secondary
                            )
                        Text(isConnected ? "Credentials connected" : "Credentials not connected")
                    } else {
                        Image(systemName: "arrow.up.and.down")
                        Text("Choose which imported context to connect")
                    }
                }
                .font(.caption)
                .foregroundStyle(.secondary)
                .accessibilityElement(children: .combine)
                .accessibilityLabel(profileAccessibilityLabel)
            }
        }
        .padding(10)
        .background(RuneSurfaceBackground(kind: .inset))
        .overlay {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .strokeBorder(Color(nsColor: .separatorColor).opacity(0.16), lineWidth: 1)
        }
    }

    private var profileAccessibilityLabel: String {
        guard !isCheckingProfiles else { return "Checking native credential profile" }
        guard let selectedOption else { return "Choose a compatible native authentication context" }
        return connectedBindingIDs.contains(selectedOption.id)
            ? "Native credentials connected for \(selectedOption.contextName)"
            : "Native credentials not connected for \(selectedOption.contextName)"
    }
}
