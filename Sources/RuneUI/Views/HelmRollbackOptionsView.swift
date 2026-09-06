import SwiftUI

struct HelmRollbackOptionsView: View {
    @Binding var wait: Bool
    @Binding var timeout: String
    @Binding var cleanupOnFail: Bool

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Rollback options")
                .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                .foregroundStyle(.runeSecondary)
            RuneAdaptiveFormRow {
                Toggle("Wait", isOn: $wait)
                    .toggleStyle(.checkbox)
                Toggle("Cleanup on fail", isOn: $cleanupOnFail)
                    .toggleStyle(.checkbox)
                HStack(spacing: RuneUILayoutMetrics.dialogControlSpacing) {
                    Text("Timeout")
                        .foregroundStyle(.runeSecondary)
                    TextField("5m", text: $timeout)
                        .textFieldStyle(.roundedBorder)
                        .runeTextInputCursor()
                        .frame(width: 86)
                        .accessibilityLabel("Rollback timeout")
                        .help("Maximum wait duration, for example 5m or 30s.")
                }
            }
            .runeInterfaceFont(relativeSize: -1)
        }
    }
}
