import SwiftUI

struct HelmRollbackOptionsView: View {
    @Binding var wait: Bool
    @Binding var timeout: String
    @Binding var cleanupOnFail: Bool

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Rollback options")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            HStack(spacing: 12) {
                Toggle("Wait", isOn: $wait)
                    .toggleStyle(.checkbox)
                Toggle("Cleanup on fail", isOn: $cleanupOnFail)
                    .toggleStyle(.checkbox)
                TextField("Timeout", text: $timeout)
                    .textFieldStyle(.roundedBorder)
                    .frame(width: 86)
            }
            .font(.caption)
        }
    }
}
