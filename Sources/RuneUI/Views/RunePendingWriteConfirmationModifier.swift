import SwiftUI

struct RunePendingWriteConfirmationModifier: ViewModifier {
    @Binding var isPresented: Bool
    let title: String
    let confirmLabel: String
    let isDestructive: Bool
    let message: String
    let showsCopyCommandAction: Bool
    let onConfirm: () -> Void
    let onCancel: () -> Void
    let onCopyCommand: () -> Void

    func body(content: Content) -> some View {
        content.confirmationDialog(
            title,
            isPresented: $isPresented,
            titleVisibility: .visible
        ) {
            if isDestructive {
                Button(confirmLabel, role: .destructive, action: onConfirm)
            } else {
                Button(confirmLabel, action: onConfirm)
            }

            Button("Cancel", role: .cancel, action: onCancel)

            if showsCopyCommandAction {
                Button("Copy kubectl command", action: onCopyCommand)
            }
        } message: {
            Text(message)
        }
    }
}

extension View {
    func runePendingWriteConfirmation(
        isPresented: Binding<Bool>,
        title: String,
        confirmLabel: String,
        isDestructive: Bool,
        message: String,
        showsCopyCommandAction: Bool,
        onConfirm: @escaping () -> Void,
        onCancel: @escaping () -> Void,
        onCopyCommand: @escaping () -> Void
    ) -> some View {
        modifier(RunePendingWriteConfirmationModifier(
            isPresented: isPresented,
            title: title,
            confirmLabel: confirmLabel,
            isDestructive: isDestructive,
            message: message,
            showsCopyCommandAction: showsCopyCommandAction,
            onConfirm: onConfirm,
            onCancel: onCancel,
            onCopyCommand: onCopyCommand
        ))
    }
}
