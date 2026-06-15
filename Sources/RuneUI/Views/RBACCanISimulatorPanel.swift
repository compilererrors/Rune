import SwiftUI

struct RBACCanISimulatorPanel: View {
    @ObservedObject var viewModel: RuneAppViewModel

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            header
            fields
            result
        }
        .padding(12)
        .background(RuneSurfaceBackground(kind: .panel))
    }

    private var header: some View {
        HStack(alignment: .center, spacing: 8) {
            Label("Can I?", systemImage: "checkmark.shield")
                .font(.subheadline.weight(.semibold))
            Spacer(minLength: 0)
            Button("Use Selected") {
                viewModel.useSelectedRBACResourceForCanI()
            }
            .buttonStyle(.bordered)
            .controlSize(.small)
            .disabled(viewModel.state.selectedRBACResource == nil)
            Button(viewModel.isRunningRBACCanI ? "Checking..." : "Check") {
                viewModel.runRBACCanISimulator()
            }
            .buttonStyle(.borderedProminent)
            .controlSize(.small)
            .disabled(viewModel.isRunningRBACCanI)
        }
    }

    private var fields: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .center, spacing: 8) {
                TextField("Verb", text: $viewModel.rbacCanIVerb)
                    .textFieldStyle(.roundedBorder)
                    .frame(width: 86)
                TextField("Resource", text: $viewModel.rbacCanIResource)
                    .textFieldStyle(.roundedBorder)
                    .frame(minWidth: 130)
                Picker("Scope", selection: $viewModel.rbacCanIScope) {
                    ForEach(RBACCanIScope.allCases) { scope in
                        Text(scope.title).tag(scope)
                    }
                }
                .pickerStyle(.segmented)
                .labelsHidden()
                .frame(width: 170)
            }

            HStack(alignment: .center, spacing: 8) {
                TextField("API group", text: $viewModel.rbacCanIApiGroup)
                    .textFieldStyle(.roundedBorder)
                    .frame(minWidth: 150)
                TextField("Subresource", text: $viewModel.rbacCanISubresource)
                    .textFieldStyle(.roundedBorder)
                    .frame(minWidth: 120)
            }
        }
        .controlSize(.small)
    }

    @ViewBuilder
    private var result: some View {
        if let result = viewModel.rbacCanIResult {
            HStack(alignment: .top, spacing: 8) {
                Image(systemName: resultSymbol(result))
                    .foregroundStyle(resultColor(result))
                    .frame(width: 16)
                VStack(alignment: .leading, spacing: 2) {
                    Text(result.statusText)
                        .font(.caption.weight(.semibold))
                    Text(result.errorMessage ?? result.request.summary)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }
        } else {
            Text("Runs a read-only SelfSubjectAccessReview for the selected verb, resource, and scope.")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }

    private func resultSymbol(_ result: RBACCanIResult) -> String {
        if let allowed = result.allowed {
            return allowed ? "checkmark.circle.fill" : "xmark.octagon.fill"
        }
        return "exclamationmark.triangle.fill"
    }

    private func resultColor(_ result: RBACCanIResult) -> Color {
        if let allowed = result.allowed {
            return allowed ? .green : .red
        }
        return .orange
    }
}
