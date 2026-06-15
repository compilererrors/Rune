import SwiftUI

struct WorkloadReplicaScaleControlsView: View {
    let label: String
    let isDirty: Bool
    let canMutate: Bool
    @Binding var replicas: Int
    let action: () -> Void

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(alignment: .center, spacing: 8) {
                Text(label)
                    .font(.subheadline.weight(.medium))
                    .foregroundStyle(.secondary)
                Stepper(value: $replicas, in: 0...500) {
                    Text("\(replicas)")
                        .monospacedDigit()
                        .font(.body.weight(.medium))
                        .frame(minWidth: 32, alignment: .trailing)
                }
                Group {
                    if isDirty && canMutate {
                        Button("Scale") {
                            action()
                        }
                        .buttonStyle(.borderedProminent)
                    } else {
                        Button("Scale") {
                            action()
                        }
                        .buttonStyle(.bordered)
                    }
                }
                .disabled(!canMutate || !isDirty)
            }
            .fixedSize(horizontal: true, vertical: false)
        }
    }
}
