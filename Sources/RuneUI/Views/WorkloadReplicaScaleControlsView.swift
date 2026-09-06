import SwiftUI

struct WorkloadReplicaScaleControlsView: View {
    let label: String
    let isDirty: Bool
    let canMutate: Bool
    @Binding var replicas: Int
    let action: () -> Void

    var body: some View {
        RuneAdaptiveFormRow {
            Text(label)
                .runeInterfaceFont(weight: .medium)
                .foregroundStyle(.runeSecondary)
            Stepper(value: $replicas, in: 0...500) {
                Text("\(replicas)")
                    .monospacedDigit()
                    .runeInterfaceFont(weight: .medium)
                    .frame(minWidth: 32, alignment: .trailing)
            }
            .accessibilityLabel(label)
            .accessibilityValue("\(replicas)")
            .fixedSize()
            Group {
                if isDirty && canMutate {
                    Button("Scale") {
                        action()
                    }
                    .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
                } else {
                    Button("Scale") {
                        action()
                    }
                    .buttonStyle(RuneToolbarButtonStyle())
                }
            }
            .disabled(!canMutate || !isDirty)
        }
    }
}
