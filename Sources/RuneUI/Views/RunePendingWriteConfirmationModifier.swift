import AppKit
import SwiftUI

struct RunePendingWriteConfirmationModifier: ViewModifier {
    @Binding var isPresented: Bool
    let title: String
    let confirmLabel: String
    let isDestructive: Bool
    let message: String
    let targetSummary: String
    let commandPreview: String
    let showsCopyCommandAction: Bool
    let onConfirm: () -> Void
    let onCancel: () -> Void
    let onCopyCommand: () -> Void

    func body(content: Content) -> some View {
        content.sheet(isPresented: $isPresented) {
            RunePendingWriteConfirmationSheet(
                title: title,
                confirmLabel: confirmLabel,
                isDestructive: isDestructive,
                message: message,
                targetSummary: targetSummary,
                commandPreview: commandPreview,
                showsCopyCommandAction: showsCopyCommandAction,
                onConfirm: onConfirm,
                onCancel: onCancel,
                onCopyCommand: onCopyCommand
            )
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
        targetSummary: String = "",
        commandPreview: String,
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
            targetSummary: targetSummary,
            commandPreview: commandPreview,
            showsCopyCommandAction: showsCopyCommandAction,
            onConfirm: onConfirm,
            onCancel: onCancel,
            onCopyCommand: onCopyCommand
        ))
    }
}

/// Copying or inspecting the preview keeps the pending operation available.
/// The owner dismisses only after cancellation or its final confirmation step.
struct RunePendingWriteConfirmationSheet: View {
    let title: String
    let confirmLabel: String
    let isDestructive: Bool
    let message: String
    var targetSummary: String = ""
    let commandPreview: String
    let showsCopyCommandAction: Bool
    let onConfirm: () -> Void
    let onCancel: () -> Void
    let onCopyCommand: () -> Void
    @Environment(\.runeThemePalette) private var runeThemePalette
    @State private var isConfirming = false
    @State private var confirmationCooldownTask: Task<Void, Never>?

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            HStack(alignment: .top, spacing: 12) {
                Image(systemName: isDestructive ? "exclamationmark.triangle.fill" : "checkmark.shield")
                    .font(.title2)
                    .foregroundStyle(
                        isDestructive
                            ? RuneSemanticColorRole.danger.color(in: runeThemePalette)
                            : RuneSemanticColorRole.info.color(in: runeThemePalette)
                    )
                    .frame(width: 28, height: 28)
                Text(title)
                    .runeInterfaceFont(relativeSize: 2, weight: .semibold)
                    .fixedSize(horizontal: false, vertical: true)
                    .accessibilityAddTraits(.isHeader)
                Spacer(minLength: 0)
                RuneDialogCloseButton("Cancel operation", action: onCancel)
            }
            .padding(RuneUILayoutMetrics.dialogContentPadding)

            Divider()

            ScrollView {
                VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogSectionSpacing) {
                    if !targetSummary.isEmpty {
                        Text(targetSummary)
                            .runeInterfaceFont(relativeSize: -1, weight: .medium)
                            .fixedSize(horizontal: false, vertical: true)
                            .textSelection(.enabled)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .runeInsetCard(padding: 12)
                            .accessibilityIdentifier("rune.write-review.target")
                    }
                    Text(message)
                        .runeInterfaceFont()
                        .fixedSize(horizontal: false, vertical: true)
                        .textSelection(.enabled)

                    if !commandPreview.isEmpty {
                        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogControlSpacing) {
                            Text("Command preview")
                                .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                                .foregroundStyle(.secondary)
                            Text(commandPreview)
                                .runeInterfaceFont(relativeSize: -1, design: .monospaced)
                                .fixedSize(horizontal: false, vertical: true)
                                .textSelection(.enabled)
                                .frame(maxWidth: .infinity, alignment: .leading)
                                .runeInsetCard(padding: 12)
                        }
                    }
                    if showsCopyCommandAction {
                        Button("Copy command", action: onCopyCommand)
                            .buttonStyle(.bordered)
                            .accessibilityIdentifier("rune.write-review.copy-command")
                    }
                }
                .padding(RuneUILayoutMetrics.dialogContentPadding)
                .frame(maxWidth: .infinity, alignment: .leading)
            }
            .frame(minHeight: 100, idealHeight: 240, maxHeight: 380)

            RuneDialogActionBar {
                Button(role: .cancel, action: onCancel) {
                    RuneDialogButtonLabel("Cancel")
                }
                .buttonStyle(.bordered)
                .keyboardShortcut(.cancelAction)
                Button(role: isDestructive ? .destructive : nil, action: confirm) {
                    RuneDialogButtonLabel(confirmLabel)
                }
                .buttonStyle(.borderedProminent)
                .tint(isDestructive
                    ? RuneSemanticColorRole.danger.color(in: runeThemePalette)
                    : RuneSemanticColorRole.info.color(in: runeThemePalette))
                .disabled(isConfirming)
                .accessibilityIdentifier("rune.write-review.confirm")
            }
            .padding(.horizontal, RuneUILayoutMetrics.dialogContentPadding)
            .padding(.bottom, RuneUILayoutMetrics.dialogContentPadding)
        }
        .frame(minWidth: RuneUILayoutMetrics.compactDialogWidth, idealWidth: RuneUILayoutMetrics.standardDialogWidth,
               maxWidth: RuneUILayoutMetrics.standardDialogWidth)
        .frame(maxHeight: RuneUILayoutMetrics.providerDialogMaxHeight)
        .runePointerCursor()
        .onDisappear {
            confirmationCooldownTask?.cancel()
            confirmationCooldownTask = nil
            isConfirming = false
        }
    }

    private func confirm() {
        guard !isConfirming else { return }
        isConfirming = true
        onConfirm()

        // A production action can stay presented for its final confirmation.
        // A double click must never count as both review and execution.
        confirmationCooldownTask = Task { @MainActor in
            do {
                try await Task.sleep(for: .seconds(NSEvent.doubleClickInterval + 0.1))
                guard !Task.isCancelled else { return }
                isConfirming = false
                confirmationCooldownTask = nil
            } catch {
                // Dismissing the sheet cancels its pending cooldown.
            }
        }
    }
}
